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

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_bucket', 'ops_database', 'output_bucket'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

OPS_DB            = args['ops_database']
ICEBERG_WAREHOUSE = f"s3://{args['iceberg_bucket']}/"
OUTPUT_BUCKET     = args['output_bucket']

CATALOG           = "glue_catalog"
TABLE_INVENTORY   = f"{CATALOG}.{OPS_DB}.table_inventory"

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)


def main():
    logger.info("=== Inventory Reader — Reading active subscriptions ===")

    # Read active subscriptions
    rows = (
        spark.table(TABLE_INVENTORY)
             .filter(F.col("is_active") == True)
             .collect()
    )

    logger.info("Found %d active subscription(s).", len(rows))

    if not rows:
        logger.info("No active subscriptions — nothing to compact.")
        # Write empty output so Map state doesn't fail
        _write_output([])
        job.commit()
        return

    # Build work item list for Step Function Map state
    work_items = [
        {
            "subscription_name":      row["subscription_name"],
            "topic_name":             row["topic_name"],
            "ingestion_mode":         row["ingestion_mode"] or "STREAMING",
            "raw_database":           row["raw_database"],
            "raw_table":              row["raw_table"],
            "standardized_database":  row["standardized_database"],
            "standardized_table":     row["standardized_table"],
            "curated_database":       row["curated_database"],
            "curated_table":          row["curated_table"],
        }
        for row in rows
    ]

    # Write JSON list to S3 for Step Function to pick up
    _write_output(work_items)

    logger.info("Inventory reader complete. Emitted %d work items.", len(work_items))
    job.commit()


def _write_output(work_items: list):
    """
    Writes the work_items array to a fixed S3 path.
    The Step Function reads this file between the ReadInventory and Map states.
    """
    s3 = boto3.client('s3')
    payload = json.dumps({"work_items": work_items}, indent=2)
    key = "maintenance/work_items.json"
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=key, Body=payload)
    logger.info("Work items written to s3://%s/%s", OUTPUT_BUCKET, key)


if __name__ == "__main__":
    main()
