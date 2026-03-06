"""
==============================================================================
CURATED LAYER PROCESSOR (Glue V2)
==============================================================================
Reads incrementally from the pre-built Standardized Iceberg table using a
timestamp-based DynamoDB checkpoint, applies CDE validation, strong type casting,
and writes to the pre-built Curated Iceberg table via MERGE.

NOTE: Curated uses an ingestion TIMESTAMP checkpoint (not a snapshot ID).
      This is because MERGE operations invalidate Iceberg snapshots, making
      snapshot-based incremental reading unreliable for a table that is
      constantly merged. Using the `ingestion_ts` column from the Standardized
      table (which is monotonically increasing and never changes post-insert)
      is the correct approach here.

Flow:
  STANDARDIZED (all-STRING, flat columns)
    → [Timestamp Read]       — Read rows where ingestion_ts > last checkpoint
    → [CDE Validation]       — Route records missing critical fields to errors table
    → [Type Casting]         — Convert STRING to INT/TIMESTAMP/DECIMAL per schema
    → [Audit Columns]        — Append first_seen_ts, last_updated_ts, _schema_version
    → [Column Projection]    — Select only schema-defined columns
    → [MERGE Into Curated]   — Upsert (LIFO: update only if incoming is newer)
    → [DynamoDB Checkpoint]  — Bookmark the max ingestion_ts seen this run

Assumptions:
    - Both the Curated and Errors tables already exist (pre-built).
    - The schema JSON is loaded from S3 and defines columns, CDEs, and audit metadata.
    - All columns entering from Standardized are STRING type.

Inputs (Step Function / Glue args):
    topic_name, standardized_database, curated_database,
    checkpoint_table, iceberg_bucket, schema_bucket
"""

import sys
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import boto3

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS
from utils.transforms import cast_types, add_audit_columns
from utils.validation import validate_cdes

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Glue Job Initialisation
# ---------------------------------------------------------------------------

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'topic_name',
    'standardized_database',
    'curated_database',
    'checkpoint_table',
    'iceberg_bucket',
    'schema_bucket',
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TOPIC_NAME            = args['topic_name']
STANDARDIZED_DATABASE = args['standardized_database']
CURATED_DATABASE      = args['curated_database']
CHECKPOINT_TABLE      = args['checkpoint_table']
SCHEMA_BUCKET         = args['schema_bucket']
ICEBERG_WAREHOUSE     = f"s3://{args['iceberg_bucket']}/"

STANDARDIZED_TABLE    = f"glue_catalog.{STANDARDIZED_DATABASE}.{TOPIC_NAME}"
CURATED_TABLE         = f"glue_catalog.{CURATED_DATABASE}.{TOPIC_NAME}"
ERRORS_TABLE          = f"glue_catalog.{CURATED_DATABASE}.errors"

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)


# ---------------------------------------------------------------------------
# Schema Loading
# ---------------------------------------------------------------------------

def _load_schema(bucket: str, topic: str) -> dict:
    """
    Loads the curated schema JSON from S3.
    Expected path: s3://{bucket}/schemas/{topic}.json

    Schema format:
    {
      "version": "1.0",
      "primary_key": "idempotency_key",
      "columns": {
          "idempotency_key":  {"type": "STRING",       "cde": true,  "nullable": false},
          "amount":           {"type": "DECIMAL(10,2)", "cde": false},
          "event_timestamp":  {"type": "TIMESTAMP",    "cde": true,  "nullable": false}
      },
      "audit_columns": {
          "_schema_version":  {"type": "STRING",    "default": "1.0"},
          "first_seen_ts":    {"type": "TIMESTAMP"},
          "last_updated_ts":  {"type": "TIMESTAMP"}
      },
      "partitioning": {
           "fields": ["period_reference", "day(event_timestamp)"]
      }
    }
    """
    import json
    s3 = boto3.client('s3')
    key = f"schemas/{topic}.json"
    resp = s3.get_object(Bucket=bucket, Key=key)
    schema = json.loads(resp['Body'].read().decode('utf-8'))
    logger.info("Loaded schema from s3://%s/%s (%d columns)", bucket, key, len(schema.get('columns', {})))
    return schema


# ---------------------------------------------------------------------------
# DynamoDB Timestamp Checkpoint Helpers
# ---------------------------------------------------------------------------

def _get_last_ts(topic_name: str) -> int:
    """
    Returns the highest `ingestion_ts` from the previous Curated run, or 0.
    Curated tracks timestamps (not snapshot IDs) because MERGE invalidades snapshots.
    """
    dynamo = boto3.resource('dynamodb')
    try:
        resp = dynamo.Table(CHECKPOINT_TABLE).get_item(
            Key={'pipeline_id': f'curated_{topic_name}', 'checkpoint_type': 'curated'}
        )
        return int(resp.get('Item', {}).get('last_ingestion_ts', 0))
    except Exception as e:
        logger.warning("Checkpoint read failed: %s", e)
    return 0


def _save_checkpoint(topic_name: str, max_ts: int):
    dynamo = boto3.resource('dynamodb')
    dynamo.Table(CHECKPOINT_TABLE).put_item(Item={
        'pipeline_id':      f'curated_{topic_name}',
        'checkpoint_type':  'curated',
        'last_ingestion_ts': max_ts,
        'updated_at':        datetime.utcnow().isoformat(),
    })
    logger.info("Checkpoint saved: ingestion_ts=%d", max_ts)


# ---------------------------------------------------------------------------
# MERGE Write into Curated
# ---------------------------------------------------------------------------

def _write_curated(df, primary_key: str):
    """
    Upserts records into the Curated table using Iceberg MERGE.
    
    Match condition: same primary_key AND period_reference within ±1 month.
    Update condition: incoming record must be NEWER than the current version
                      (prevents stale data from overwriting corrections).
    """
    if df.count() == 0:
        logger.info("No records to write.")
        return

    df.createOrReplaceTempView("curated_batch")
    cols        = df.columns
    update_set  = ", ".join(f"t.{c} = s.{c}" for c in cols if c != primary_key)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)

    spark.sql(f"""
        MERGE INTO {CURATED_TABLE} t
        USING curated_batch s
          ON t.{primary_key} = s.{primary_key}
         AND (
               t.period_reference = s.period_reference
            OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'), -1), 'yyyy-MM')
            OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'),  1), 'yyyy-MM')
         )
        WHEN MATCHED AND s.last_updated_ts > t.last_updated_ts THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
    """)
    logger.info("MERGE complete — %d records written to %s", df.count(), CURATED_TABLE)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    current_stage = "INIT"
    try:
        logger.info("=== Curated Processor (V2) — Topic: %s ===", TOPIC_NAME)

        # Stage 1: Load Schema
        current_stage = "SCHEMA"
        schema = _load_schema(SCHEMA_BUCKET, TOPIC_NAME)
        if not schema.get('columns'):
            raise RuntimeError("Schema loaded from S3 has no 'columns' definition.")

        schema_version = schema.get('version', '1.0')
        primary_key    = schema.get('primary_key', 'idempotency_key')
        logger.info("Schema v%s, primary_key='%s'", schema_version, primary_key)

        # Stage 2: Checkpoint
        current_stage = "CHECKPOINT"
        last_ts = _get_last_ts(TOPIC_NAME)
        logger.info("Reading from Standardized where ingestion_ts > %d", last_ts)

        # Stage 3: Read Incremental from Standardized
        current_stage = "READ"
        std_df = spark.table(STANDARDIZED_TABLE).filter(
            F.col("ingestion_ts") > F.lit(last_ts)
        )
        records_in = std_df.count()
        logger.info("Records read from Standardized: %d", records_in)

        if records_in == 0:
            logger.info("No new records — exiting.")
            job.commit(); return

        max_ts = int(std_df.agg(F.max("ingestion_ts")).collect()[0][0] or last_ts)

        # Stage 4: CDE Validation
        current_stage = "VALIDATE"
        df_valid, df_errors = validate_cdes(std_df, schema)
        records_valid  = df_valid.count()
        records_errors = df_errors.count()

        if records_errors > 0:
            logger.warning("%d records failed CDE validation — routing to errors table.", records_errors)
            try:
                # Ensure errors table has standardised columns
                errors_df = df_errors.select(
                    F.col("raw_payload"),
                    F.col("error_type"),
                    F.col("error_message"),
                    F.col("processed_ts"),
                )
                errors_df.writeTo(ERRORS_TABLE).append()
            except Exception as e:
                logger.error("Failed to write errors (processing will continue): %s", e)

        if records_valid == 0:
            logger.info("No valid records after CDE check — updating checkpoint and exiting.")
            _save_checkpoint(TOPIC_NAME, max_ts)
            job.commit(); return

        # Stage 5: Type Casting
        current_stage = "CAST"
        df_typed = cast_types(df_valid, schema)

        # Stage 6: Audit Columns
        current_stage = "AUDIT"
        df_audited = add_audit_columns(df_typed, schema_version=schema_version)

        # Stage 7: Project to exact schema columns (schema_columns + audit_columns)
        schema_cols = list(schema.get('columns', {}).keys())
        audit_cols  = list(schema.get('audit_columns', {}).keys())
        keep_cols   = [c for c in schema_cols + audit_cols if c in df_audited.columns]
        df_final    = df_audited.select(*keep_cols)

        # Stage 8: MERGE Write
        current_stage = "WRITE"
        _write_curated(df_final, primary_key)

        # Stage 9: Checkpoint
        current_stage = "CHECKPOINT"
        _save_checkpoint(TOPIC_NAME, max_ts)

        logger.info(
            "=== SUMMARY === IN:%d | VALID:%d | ERRORS:%d | TABLE_TOTAL:%d",
            records_in, records_valid, records_errors,
            spark.table(CURATED_TABLE).count()
        )
        job.commit()

    except Exception as e:
        logger.error("=== JOB FAILED at stage: %s ===", current_stage)
        logger.error("Error: %s", str(e))
        raise


if __name__ == "__main__":
    main()
