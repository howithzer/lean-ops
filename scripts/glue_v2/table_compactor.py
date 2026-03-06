"""
==============================================================================
TABLE COMPACTOR — SINGLE SUBSCRIPTION (Glue V2 / Maintenance)
==============================================================================
Stage 2 of the parallel compaction Step Function (Map state branch).

One instance of this job runs per active subscription. It receives the
subscription's three table paths as Glue arguments from the Step Function
Map state, determines the appropriate compaction tier for each partition,
compacts with sorting, and logs results to maintenance_log.

TIERED COMPACTION STRATEGY:
┌─────────────┬─────────────────────┬──────────────────────┬─────────────────────────┐
│ Tier        │ Partition Age       │ Run Frequency        │ Goal                    │
├─────────────┼─────────────────────┼──────────────────────┼─────────────────────────┤
│ DAILY       │ 0-6 days (hot)      │ Every run (daily)    │ Many small files → few  │
│ WEEKLY      │ 7-89 days (warm)    │ Every 7th run (Sun.) │ Merge daily files       │
│ QUARTERLY   │ 90+ days (cold)     │ Every 90th run       │ One or two large files  │
└─────────────┴─────────────────────┴──────────────────────┴─────────────────────────┘

SORT ORDERS (applied during compaction for improved compression + query perf):
  RAW/Standardized : period_reference ASC, publish_ts ASC
  Curated           : period_reference ASC, idempotency_key ASC, dl_ingest_ts ASC

Inputs (Glue args / Step Function pass-through):
    subscription_name, topic_name,
    raw_database, raw_table,
    standardized_database, standardized_table,
    curated_database, curated_table,
    ops_database, iceberg_bucket
"""

import sys
import uuid
from datetime import datetime, timedelta, timezone, date

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS
from utils.ops_store import write_maintenance_entry

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'subscription_name',
    'topic_name',
    'raw_database',      'raw_table',
    'standardized_database', 'standardized_table',
    'curated_database',  'curated_table',
    'ops_database',
    'iceberg_bucket',
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ICEBERG_WAREHOUSE = f"s3://{args['iceberg_bucket']}/"
CATALOG           = "glue_catalog"
RUN_ID            = str(uuid.uuid4())
TODAY             = date.today()

RETENTION_DAYS    = 7   # Expire snapshots older than this

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# Sort orders per layer
SORT_ORDER = {
    "RAW":           "period_reference ASC NULLS LAST, publish_ts ASC NULLS LAST",
    "STANDARDIZED":  "period_reference ASC NULLS LAST, publish_ts ASC NULLS LAST",
    "CURATED":       "period_reference ASC NULLS LAST, idempotency_key ASC NULLS LAST, dl_ingest_ts ASC NULLS LAST",
}

# Partition columns per layer for WHERE filter and data check
PARTITION_COL = {
    "RAW":          "publish_ts",
    "STANDARDIZED": "publish_ts",
    "CURATED":      "dl_ingest_ts",
}


# ---------------------------------------------------------------------------
# Tiered Compaction Logic
# ---------------------------------------------------------------------------

def _get_partitions_to_compact(ingestion_mode: str) -> list:
    """
    Returns a list of (partition_date_str, tier) tuples for the current run.

    STREAMING topics (Firehose 64MB/150s):
      → ~576 files/day produced by Firehose → aggressive DAILY compaction essential.
      → DAILY tier runs every day for the past 7 days.

    BATCH topics (periodic file drops):
      → Files already land in large chunks → low file count even without daily compaction.
      → Skip DAILY tier; WEEKLY warm and QUARTERLY cold still apply.

    WEEKLY  (Sundays): compact warm data 7-89 days old
    QUARTERLY (Jan/Apr/Jul/Oct 1st): compact cold data 90-365 days old
    """
    partitions = []
    is_streaming = ingestion_mode.upper() != "BATCH"

    # DAILY: streaming topics only — compact the last 7 days every run
    if is_streaming:
        for delta in range(1, 8):
            d = TODAY - timedelta(days=delta)
            partitions.append((d.strftime("%Y-%m-%d"), "DAILY"))

    # WEEKLY: Sundays — warm data 7-89 days old (both modes)
    if TODAY.weekday() == 6:
        for delta in range(7, 90):
            d = TODAY - timedelta(days=delta)
            partitions.append((d.strftime("%Y-%m-%d"), "WEEKLY"))

    # QUARTERLY: cold data 90-365 days (both modes, quarter boundary only)
    month = TODAY.month
    if TODAY.day == 1 and month in (1, 4, 7, 10):
        for delta in range(90, 366):
            d = TODAY - timedelta(days=delta)
            partitions.append((d.strftime("%Y-%m-%d"), "QUARTERLY"))

    return partitions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _count_files(catalog_table: str) -> int:
    try:
        return spark.sql(f"SELECT count(*) AS cnt FROM {catalog_table}.files").first()['cnt']
    except Exception:
        return -1


def _has_data(catalog_table: str, partition_col: str, partition_date: str) -> bool:
    try:
        cnt = spark.sql(
            f"SELECT count(*) AS cnt FROM {catalog_table}"
            f" WHERE date({partition_col}) = date('{partition_date}')"
        ).first()['cnt']
        return cnt > 0
    except Exception:
        return False


def _expire_snapshots(database: str, table: str):
    expire_before = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime(
        "%Y-%m-%dT%H:%M:%S.000"
    )
    try:
        spark.sql(f"""
            CALL {CATALOG}.system.expire_snapshots(
                table       => '{database}.{table}',
                older_than  => TIMESTAMP '{expire_before}',
                retain_last => 1
            )
        """)
    except Exception as e:
        logger.warning("expire_snapshots failed for %s.%s: %s", database, table, e)


# ---------------------------------------------------------------------------
# Core Compaction
# ---------------------------------------------------------------------------

def compact_one(database: str, table: str, layer: str, partition_date: str, tier: str):
    """
    Compacts a single partition of a single table with sort order applied.
    Writes one row to maintenance_log.
    """
    catalog_table = f"{CATALOG}.{database}.{table}"
    partition_col = PARTITION_COL[layer]
    sort_order    = SORT_ORDER[layer]
    start_ms      = int(datetime.now(timezone.utc).timestamp() * 1000)

    if not _has_data(catalog_table, partition_col, partition_date):
        write_maintenance_entry(
            spark, RUN_ID, database, table, layer, partition_date,
            0, 0, "SKIPPED", 0,
            error_message=f"No data for partition {partition_date} (tier={tier})"
        )
        logger.info("[%s][%s] SKIPPED %s.%s — no data", tier, layer, database, table)
        return

    try:
        files_before = _count_files(catalog_table)

        spark.sql(f"""
            CALL {CATALOG}.system.rewrite_data_files(
                table       => '{database}.{table}',
                strategy    => 'sort',
                sort_order  => '{sort_order}',
                where       => 'date({partition_col}) = date("{partition_date}")'
            )
        """)

        files_after = _count_files(catalog_table)
        duration_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - start_ms

        write_maintenance_entry(
            spark, RUN_ID, database, table, layer, partition_date,
            files_before, files_after, "SUCCESS", duration_ms,
            error_message=f"tier={tier}"
        )
        logger.info(
            "[%s][%s] SUCCESS %s.%s partition=%s | files %d→%d (%dms)|sort: %s",
            tier, layer, database, table, partition_date,
            files_before, files_after, duration_ms, sort_order
        )

    except Exception as e:
        duration_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - start_ms
        write_maintenance_entry(
            spark, RUN_ID, database, table, layer, partition_date,
            -1, -1, "FAILED", duration_ms, error_message=str(e)[:2000]
        )
        logger.error("[%s][%s] FAILED %s.%s partition=%s: %s",
                     tier, layer, database, table, partition_date, e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    topic          = args['topic_name']
    ingestion_mode = args.get('ingestion_mode', 'STREAMING').upper()
    logger.info("=== Table Compactor — Subscription: %s | Mode: %s | Run: %s ===",
                topic, ingestion_mode, RUN_ID)

    tables = [
        (args['raw_database'],          args['raw_table'],          "RAW"),
        (args['standardized_database'], args['standardized_table'], "STANDARDIZED"),
        (args['curated_database'],      args['curated_table'],      "CURATED"),
    ]

    # Get all partitions to compact based on ingestion mode + schedule tier
    partitions = _get_partitions_to_compact(ingestion_mode)
    logger.info("Partitions to compact this run: %d (mode=%s)", len(partitions), ingestion_mode)

    for database, table, layer in tables:
        logger.info("--- Processing [%s] %s.%s ---", layer, database, table)
        for partition_date, tier in partitions:
            compact_one(database, table, layer, partition_date, tier)

        # Expire old snapshots once per table (not per partition)
        _expire_snapshots(database, table)

    logger.info("=== Compactor complete for subscription: %s ===", topic)
    job.commit()


if __name__ == "__main__":
    main()
