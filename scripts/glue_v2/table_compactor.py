"""
==============================================================================
TABLE COMPACTOR (Glue V2)
==============================================================================
Reads the subscription inventory from `operational_data_store.table_inventory`,
compacts yesterday's partitions across all active RAW, Standardized, and Curated
Iceberg tables, and records results to `maintenance_log`.

Orchestration:
    EventBridge (daily 03:00 AM) → Step Function → This Glue Job

Strategy:
    - RAW tables: auto-discovered from Glue Catalog (all tables in raw_database).
      New topics are automatically included without any manual inventory step.
    - Standardized / Curated tables: driven by table_inventory rows.
    - Compaction scope: yesterday's partition only, to avoid disrupting
      today's active writes.
    - File compaction: Iceberg `rewrite_data_files` with binpack strategy.
    - Snapshot expiry: `expire_snapshots` retains the last N days (default 7).

Inputs (Glue args):
    raw_database, ops_database, iceberg_bucket, snapshot_retention_days
"""

import sys
import uuid
from datetime import datetime, timedelta, timezone, date

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS
from utils.ops_store import get_active_inventory, write_maintenance_entry

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'raw_database',             # e.g. 'iceberg_raw_db'
    'ops_database',             # e.g. 'operational_data_store'
    'iceberg_bucket',
    'snapshot_retention_days',  # e.g. '7'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RAW_DATABASE         = args['raw_database']
OPS_DB               = args['ops_database']
ICEBERG_WAREHOUSE    = f"s3://{args['iceberg_bucket']}/"
RETENTION_DAYS       = int(args.get('snapshot_retention_days', '7'))
CATALOG              = "glue_catalog"

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# Target: yesterday's partition date
YESTERDAY = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
EXPIRE_BEFORE = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).strftime(
    "%Y-%m-%dT%H:%M:%S.000"
)

RUN_ID = str(uuid.uuid4())
logger.info("=== Table Compactor — Run ID: %s — Partition: %s ===", RUN_ID, YESTERDAY)


# ---------------------------------------------------------------------------
# File Count Helper
# ---------------------------------------------------------------------------

def _count_files(catalog_table: str) -> int:
    """
    Returns the number of data files currently tracked by the Iceberg table.
    Used to compute files_before and files_after metrics.
    """
    try:
        return spark.sql(
            f"SELECT count(*) AS cnt FROM {catalog_table}.files"
        ).first()['cnt']
    except Exception:
        return -1  # -1 indicates unknown (stats not critical for function)


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

def compact_table(database: str, table: str, layer: str, partition_date: str, partition_col: str):
    """
    Runs Iceberg rewrite_data_files on yesterday's partition for one table.
    `partition_col` is the day()-partitioned column for this layer:
      - RAW / STANDARDIZED : 'publish_ts'
      - CURATED             : 'dl_ingest_ts'
    Using the actual partition column allows Iceberg to prune at partition
    level rather than scanning the whole table.
    """
    catalog_table = f"{CATALOG}.{database}.{table}"
    start_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    try:
        files_before = _count_files(catalog_table)

        # Compact only yesterday's partition — use the actual day() partition
        # column so Iceberg prunes at partition level (no full table scan)
        spark.sql(f"""
            CALL {CATALOG}.system.rewrite_data_files(
                table    => '{database}.{table}',
                strategy => 'binpack',
                where    => 'date({partition_col}) = date("{partition_date}")'
            )
        """)
        logger.info("[%s] Compacted %s (partition: %s)", layer, catalog_table, partition_date)

        # Expire old snapshots beyond retention window
        spark.sql(f"""
            CALL {CATALOG}.system.expire_snapshots(
                table          => '{database}.{table}',
                older_than     => TIMESTAMP '{EXPIRE_BEFORE}',
                retain_last    => 1
            )
        """)
        logger.info("[%s] Expired snapshots older than %s from %s", layer, EXPIRE_BEFORE, catalog_table)

        files_after = _count_files(catalog_table)
        duration_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - start_ms

        write_maintenance_entry(
            spark, RUN_ID, database, table, layer, partition_date,
            files_before, files_after, "SUCCESS", duration_ms
        )

    except Exception as e:
        duration_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - start_ms
        logger.error("[%s] Compaction failed for %s.%s: %s", layer, database, table, e)
        write_maintenance_entry(
            spark, RUN_ID, database, table, layer, partition_date,
            -1, -1, "FAILED", duration_ms, error_message=str(e)[:2000]
        )


def skip_table(database: str, table: str, layer: str, reason: str):
    """Writes a SKIPPED entry to maintenance_log (e.g., no data for yesterday)."""
    logger.info("[%s] Skipping %s.%s — %s", layer, database, table, reason)
    write_maintenance_entry(
        spark, RUN_ID, database, table, layer, YESTERDAY,
        0, 0, "SKIPPED", 0, error_message=reason
    )


# ---------------------------------------------------------------------------
# Partition Existence Check
# ---------------------------------------------------------------------------

def _has_yesterday_data(catalog_table: str, partition_col: str) -> bool:
    """
    Returns True if the table has any data in yesterday's partition.
    Avoids running rewrite_data_files on empty partitions (which would fail).
    `partition_col` must be the actual day() partition column so Iceberg can
    prune at the partition level without a full scan.
    """
    try:
        count = spark.sql(
            f"SELECT count(*) AS cnt FROM {catalog_table} WHERE date({partition_col}) = date('{YESTERDAY}')"
        ).first()['cnt']
        return count > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    totals = {"success": 0, "skipped": 0, "failed": 0}

    # Partition columns per layer
    # RAW + STANDARDIZED : partitioned by period_reference, day(publish_ts)
    # CURATED            : partitioned by period_reference, day(publish_ts), day(dl_ingest_ts)
    # Use the day()-partitioned column for both the existence check and compaction
    # WHERE filter so Iceberg can prune at partition level.
    RAW_STD_PART_COL = "publish_ts"
    CURATED_PART_COL = "dl_ingest_ts"

    # ── 1. Compact RAW Tables (auto-discovered) ──────────────────────────────
    logger.info("--- Discovering RAW tables in '%s' ---", RAW_DATABASE)
    raw_tables = spark.catalog.listTables(RAW_DATABASE)
    for t in raw_tables:
        table_name    = t.name
        catalog_table = f"{CATALOG}.{RAW_DATABASE}.{table_name}"

        if not _has_yesterday_data(catalog_table, RAW_STD_PART_COL):
            skip_table(RAW_DATABASE, table_name, "RAW", "No data for partition date")
            totals["skipped"] += 1
        else:
            compact_table(RAW_DATABASE, table_name, "RAW", YESTERDAY, RAW_STD_PART_COL)
            totals["success"] += 1

    # ── 2. Compact Std and Curated tables from inventory ─────────────────────
    logger.info("--- Reading active inventory from ODS ---")
    inventory = get_active_inventory(spark)

    for row in inventory:
        # Standardized
        std_table = f"{CATALOG}.{row.standardized_database}.{row.standardized_table}"
        if not _has_yesterday_data(std_table, RAW_STD_PART_COL):
            skip_table(row.standardized_database, row.standardized_table, "STANDARDIZED",
                       "No data for partition date")
            totals["skipped"] += 1
        else:
            compact_table(row.standardized_database, row.standardized_table,
                          "STANDARDIZED", YESTERDAY, RAW_STD_PART_COL)
            totals["success"] += 1

        # Curated — uses dl_ingest_ts as the compaction scope partition column
        cur_table = f"{CATALOG}.{row.curated_database}.{row.curated_table}"
        if not _has_yesterday_data(cur_table, CURATED_PART_COL):
            skip_table(row.curated_database, row.curated_table, "CURATED",
                       "No data for partition date")
            totals["skipped"] += 1
        else:
            compact_table(row.curated_database, row.curated_table,
                          "CURATED", YESTERDAY, CURATED_PART_COL)
            totals["success"] += 1

    logger.info(
        "=== Compaction Complete — SUCCESS:%d | SKIPPED:%d | FAILED:%d ===",
        totals["success"], totals["skipped"], totals["failed"]
    )
    job.commit()


if __name__ == "__main__":
    main()
