"""
==============================================================================
STANDARDIZED LAYER PROCESSOR (Glue V2)
==============================================================================
Reads incrementally from the RAW Iceberg table using Iceberg snapshots as
bookmarks, and writes flattened, deduplicated, ALL-STRING records into the
pre-built Standardized Iceberg table.

Flow:
  RAW (JSON payload)
    → [Snapshot Read]        — Read only records added since last run
    → [FIFO Dedup]           — Remove network-level duplicates (same message_id)
    → [LIFO Dedup]           — Keep latest business version (same idempotency_key)
    → [JSON Validation]      — Route malformed JSON to parse_errors
    → [Flatten]              — Unpack nested JSON to flat columns
    → [Schema Evolution]     — Add any new columns to the target table
    → [MERGE Into Std Table] — Upsert into Standardized (pre-built, ALL-STRING)
    → [DynamoDB Checkpoint]  — Bookmark the latest snapshot

Assumptions:
    - The Standardized table already exists (created by schema_validator Lambda
      or Terraform).
    - All output columns in the Standardized table are STRING type.
    - The table is partitioned and must be written to via MERGE (never
      createOrReplace) to preserve DDL partitioning.
    
Inputs (Step Function / Glue args):
    topic_name, raw_database, standardized_database, checkpoint_table, iceberg_bucket
"""

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import boto3

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS
from utils.flatten import flatten_json_payload
from utils.schema_evolution import (
    add_missing_columns_to_table,
    align_dataframe_to_table,
    safe_cast_to_string,
)

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Glue Job Initialisation
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'topic_name',
    'raw_database',
    'standardized_database',
    'checkpoint_table',
    'iceberg_bucket',
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TOPIC_NAME            = args['topic_name']
RAW_DATABASE          = args['raw_database']
STANDARDIZED_DATABASE = args['standardized_database']
CHECKPOINT_TABLE      = args['checkpoint_table']
ICEBERG_WAREHOUSE     = f"s3://{args['iceberg_bucket']}/"

RAW_TABLE             = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
STANDARDIZED_TABLE    = f"glue_catalog.{STANDARDIZED_DATABASE}.{TOPIC_NAME}"
PARSE_ERRORS_TABLE    = f"glue_catalog.{STANDARDIZED_DATABASE}.parse_errors"

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)


# ---------------------------------------------------------------------------
# DynamoDB Checkpoint Helpers
# ---------------------------------------------------------------------------

def _get_last_snapshot(topic_name: str) -> str:
    """Returns the last-processed RAW snapshot ID, or '0' for first run."""
    dynamo = boto3.resource('dynamodb')
    try:
        resp = dynamo.Table(CHECKPOINT_TABLE).get_item(
            Key={'pipeline_id': f'standardization_{topic_name}', 'checkpoint_type': 'standardized'}
        )
        return str(resp.get('Item', {}).get('last_snapshot_id', '0'))
    except Exception as e:
        logger.warning("Checkpoint read failed: %s", e)
    return '0'


def _get_current_snapshot(table_path: str) -> str:
    """Returns the latest RAW table snapshot ID, or '0' if table is empty."""
    try:
        row = spark.sql(
            f"SELECT snapshot_id FROM {table_path}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).first()
        return str(row['snapshot_id']) if row else '0'
    except Exception as e:
        logger.warning("Could not read snapshots from %s: %s", table_path, e)
    return '0'


def _save_checkpoint(topic_name: str, snapshot_id: str, max_ingestion_ts: int):
    dynamo = boto3.resource('dynamodb')
    dynamo.Table(CHECKPOINT_TABLE).put_item(Item={
        'pipeline_id':      f'standardization_{topic_name}',
        'checkpoint_type':  'standardized',
        'last_snapshot_id': snapshot_id,
        'last_ingestion_ts': max_ingestion_ts,
        'updated_at':        datetime.utcnow().isoformat(),
    })
    logger.info("Checkpoint saved: snapshot=%s", snapshot_id)


# ---------------------------------------------------------------------------
# Incremental Read
# ---------------------------------------------------------------------------

def _read_incremental(table_path: str, start_snapshot: str, end_snapshot: str):
    """
    Reads only the records added to the RAW table between two snapshots.
    On first run (start_snapshot == '0'), reads the entire table.
    Returns None if nothing has changed.
    """
    if end_snapshot == start_snapshot:
        logger.info("No new snapshots since last run.")
        return None

    if start_snapshot == '0' or start_snapshot is None:
        logger.info("First run: reading entire table.")
        return spark.read.format("iceberg").load(table_path)

    logger.info("Reading incremental RAW data (%s, %s]", start_snapshot, end_snapshot)
    return (
        spark.read.format("iceberg")
             .option("start-snapshot-id", start_snapshot)
             .option("end-snapshot-id", end_snapshot)
             .load(table_path)
    )


# ---------------------------------------------------------------------------
# Deduplication (Two-Stage)
# ---------------------------------------------------------------------------

def _dedup(df):
    """
    Stage 1 — FIFO: Keep the first arrival per message_id
              (removes Lambda retry duplicates).
    Stage 2 — LIFO: Keep the latest version per idempotency_key
              (applies business corrections / amendments).
    """
    logger.info("Starting two-stage deduplication...")
    count_in = df.count()

    # Stage 1: FIFO by message_id
    w_fifo = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    df_fifo = (df.withColumn("rn", F.row_number().over(w_fifo))
                 .filter(F.col("rn") == 1)
                 .drop("rn"))

    # Stage 2: LIFO by idempotency_key, using last_updated_ts with publish_time fallback
    final_cols = df_fifo.columns
    df_ts = df_fifo.withColumn(
        "_sort_ts",
        F.coalesce(
            F.get_json_object(F.col("json_payload"), "$.last_updated_ts"),
            F.col("publish_time"),
            F.col("ingestion_ts").cast("string"),
        )
    )
    w_lifo = Window.partitionBy("idempotency_key").orderBy(F.col("_sort_ts").desc())
    df_lifo = (df_ts.withColumn("rn", F.row_number().over(w_lifo))
                    .filter(F.col("rn") == 1)
                    .select(*final_cols))

    logger.info("Dedup: %d in → %d FIFO → %d LIFO", count_in, df_fifo.count(), df_lifo.count())
    return df_lifo


# ---------------------------------------------------------------------------
# JSON Validation
# ---------------------------------------------------------------------------

def _split_valid_json(df):
    """
    Splits records into valid/invalid based on whether json_payload starts and ends
    with { } or [ ]. Returns (valid_df, invalid_df).
    """
    trimmed = F.trim(F.col("json_payload"))
    valid_cond = (
        F.col("json_payload").isNotNull() &
        (trimmed != "") &
        (
            (trimmed.startswith("{") & trimmed.endswith("}")) |
            (trimmed.startswith("[") & trimmed.endswith("]"))
        )
    )
    df_checked = df.withColumn("_is_valid_json", valid_cond)
    return (
        df_checked.filter(F.col("_is_valid_json")).drop("_is_valid_json"),
        df_checked.filter(~F.col("_is_valid_json")).drop("_is_valid_json"),
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    current_stage = "INIT"
    try:
        logger.info("=== Standardized Processor (V2) — Topic: %s ===", TOPIC_NAME)

        # Stage 1: Checkpoint
        last_snapshot    = _get_last_snapshot(TOPIC_NAME)
        current_snapshot = _get_current_snapshot(RAW_TABLE)
        logger.info("Snapshots: last=%s current=%s", last_snapshot, current_snapshot)

        if current_snapshot == '0':
            logger.info("RAW table is empty — nothing to do.")
            job.commit(); return

        # Stage 2: Read
        current_stage = "READ"
        raw_df = _read_incremental(RAW_TABLE, last_snapshot, current_snapshot)
        if raw_df is None:
            logger.info("No new data — exiting.")
            job.commit(); return

        records_in = raw_df.count()
        if records_in == 0:
            _save_checkpoint(TOPIC_NAME, current_snapshot, 0)
            job.commit(); return

        max_ingestion_ts = int(raw_df.agg(F.max("ingestion_ts")).collect()[0][0] or 0)

        # Stage 3: Deduplication
        current_stage = "DEDUP"
        df_deduped = _dedup(raw_df)

        # Stage 4: JSON Validation
        current_stage = "VALIDATE"
        df_valid, df_invalid = _split_valid_json(df_deduped)
        invalid_count = df_invalid.count()
        if invalid_count > 0:
            logger.warning("Routing %d malformed JSON records to parse_errors.", invalid_count)
            df_invalid.select(
                F.col("json_payload").alias("raw_payload"),
                F.lit("INVALID_JSON").alias("error_type"),
                F.lit("JSON validation failed").alias("error_message"),
                F.current_timestamp().alias("processed_ts"),
            ).writeTo(PARSE_ERRORS_TABLE).append()

        if df_valid.count() == 0:
            logger.info("No valid records after JSON check — updating checkpoint and exiting.")
            _save_checkpoint(TOPIC_NAME, current_snapshot, max_ingestion_ts)
            job.commit(); return

        # Stage 5: Flatten
        current_stage = "FLATTEN"
        df_flat = flatten_json_payload(df_valid)
        logger.info("Columns after flatten: %d", len(df_flat.columns))

        # Rename canonical envelope fields produced by the flattener
        COLUMN_MAPPINGS = {
            "_metadata_idempotencykeyresource": "idempotency_key",
            "_metadata_periodreference":        "period_reference",
            "_metadata_correlationid":          "correlation_id",
        }
        for src, dst in COLUMN_MAPPINGS.items():
            if src in df_flat.columns:
                if dst not in df_flat.columns:
                    df_flat = df_flat.withColumn(dst, F.col(src))
                else:
                    df_flat = df_flat.withColumn(dst, F.coalesce(F.col(dst), F.col(src)))
                df_flat = df_flat.drop(src)

        # Stage 6: Schema Evolution
        current_stage = "EVOLVE"
        df_flat     = safe_cast_to_string(df_flat)
        new_cols    = add_missing_columns_to_table(spark, df_flat, STANDARDIZED_TABLE)
        if new_cols:
            logger.info("Added %d new column(s) to %s", len(new_cols), STANDARDIZED_TABLE)
        df_aligned  = align_dataframe_to_table(spark, df_flat, STANDARDIZED_TABLE)

        # Stage 7: MERGE Write
        current_stage = "WRITE"
        df_aligned.createOrReplaceTempView("staged_data")
        cols         = df_aligned.columns
        update_set   = ", ".join(f"t.{c} = s.{c}" for c in cols if c != 'idempotency_key')
        insert_cols  = ", ".join(cols)
        insert_vals  = ", ".join(f"s.{c}" for c in cols)

        # Match on idempotency_key within ±1 month of period_reference to handle late corrections
        spark.sql(f"""
            MERGE INTO {STANDARDIZED_TABLE} t
            USING staged_data s
              ON t.idempotency_key = s.idempotency_key
             AND (
                   t.period_reference = s.period_reference
                OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'), -1), 'yyyy-MM')
                OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'),  1), 'yyyy-MM')
             )
            WHEN MATCHED AND s.publish_time > t.publish_time THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        logger.info("MERGE complete — %d records written.", df_aligned.count())

        # Stage 8: Checkpoint
        current_stage = "CHECKPOINT"
        _save_checkpoint(TOPIC_NAME, current_snapshot, max_ingestion_ts)

        logger.info(
            "=== SUMMARY === IN:%d | VALID:%d | INVALID:%d | TABLE_TOTAL:%d",
            records_in, df_valid.count(), invalid_count, spark.table(STANDARDIZED_TABLE).count()
        )
        job.commit()

    except Exception as e:
        logger.error("=== JOB FAILED at stage: %s ===", current_stage)
        logger.error("Error: %s", str(e))
        raise


if __name__ == "__main__":
    main()
