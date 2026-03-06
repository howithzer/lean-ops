"""
==============================================================================
INVENTORY READER (Glue V2 / Maintenance)
==============================================================================
Stage 1 of the parallel compaction Step Function.

Reads `table_inventory` from the Operational Data Store and outputs a JSON
array of work items — one per active subscription. The Step Function's Map
state receives this array and fans out to independent per-subscription
compactor Glue jobs (concurrency=10).

Each work item emitted looks like:
{
    "subscription_name":   "projects/.../subscriptions/payments",
    "topic_name":          "payments",
    "raw_database":        "iceberg_raw_db",
    "raw_table":           "payments_staging",
    "standardized_database": "iceberg_standardized_db",
    "standardized_table":  "payments",
    "curated_database":    "iceberg_curated_db",
    "curated_table":       "payments"
}

The Step Function expects the output payload under the key "work_items".

Inputs (Glue args):
    ops_database, iceberg_bucket
"""

import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import boto3

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS

logger = get_logger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_bucket', 'ops_database', 'output_bucket', 'layer'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

OPS_DB            = args['ops_database']
ICEBERG_WAREHOUSE = f"s3://{args['iceberg_bucket']}/"
OUTPUT_BUCKET     = args['output_bucket']
LAYER             = args['layer'].upper()   # RAW | STANDARDIZED | CURATED

CATALOG           = "glue_catalog"
TABLE_INVENTORY   = f"{CATALOG}.{OPS_DB}.table_inventory"

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)


def main():
    logger.info("=== Inventory Reader — Layer: %s ===", LAYER)

    rows = (
        spark.table(TABLE_INVENTORY)
             .filter(F.col("is_active") == True)
             .collect()
    )
    logger.info("Active subscriptions found: %d", len(rows))

    raw_database = args.get('raw_database', '')

    if LAYER == "RAW":
        # Auto-discover RAW tables from Glue Catalog rather than inventory
        # so new topics are always included without a manual inventory update
        raw_tables = spark.catalog.listTables(raw_database) if raw_database else []
        work_items = [
            {
                "topic_name":      t.name,
                "ingestion_mode":  next(
                    (r["ingestion_mode"] or "STREAMING" for r in rows if r["raw_table"] == t.name),
                    "STREAMING"
                ),
                "database":        raw_database,
                "table":           t.name,
                "layer":           "RAW",
            }
            for t in raw_tables
        ]
    else:
        # STANDARDIZED or CURATED — driven by table_inventory
        db_col    = "standardized_database" if LAYER == "STANDARDIZED" else "curated_database"
        table_col = "standardized_table"    if LAYER == "STANDARDIZED" else "curated_table"
        work_items = [
            {
                "topic_name":      row["topic_name"],
                "ingestion_mode":  row["ingestion_mode"] or "STREAMING",
                "database":        row[db_col],
                "table":           row[table_col],
                "layer":           LAYER,
            }
            for row in rows
        ]

    if not work_items:
        logger.info("No work items for layer %s — writing empty list.", LAYER)
    else:
        logger.info("Emitting %d work item(s) for layer %s.", len(work_items), LAYER)

    _write_output(work_items, LAYER)
    job.commit()


def _write_output(work_items: list, layer: str):
    """
    Writes the work_items array to a layer-specific S3 path.
    Each Step Function reads its own file — no cross-layer contamination.
    """
    s3 = boto3.client('s3')
    payload = json.dumps({"work_items": work_items}, indent=2)
    key = f"maintenance/work_items_{layer.lower()}.json"
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=key, Body=payload)
    logger.info("Work items written to s3://%s/%s", OUTPUT_BUCKET, key)


if __name__ == "__main__":
    main()
