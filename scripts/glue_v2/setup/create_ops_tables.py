"""
==============================================================================
CREATE OPERATIONAL DATA STORE TABLES (One-time setup Glue Job)
==============================================================================
Creates the two Iceberg tables in `operational_data_store` if they do not exist.
Run once during initial deployment. Safe to re-run (uses CREATE TABLE IF NOT EXISTS).

Tables created:
  - table_inventory   : Subscription-centric pipeline registry
  - maintenance_log   : Per-table compaction audit log
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS

logger = get_logger(__name__)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'iceberg_bucket', 'ops_database'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

ICEBERG_WAREHOUSE = f"s3://{args['iceberg_bucket']}/"
OPS_DB            = args['ops_database']   # 'operational_data_store'
CATALOG           = "glue_catalog"

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)


def create_database():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{OPS_DB}")
    logger.info("Database '%s' ensured.", OPS_DB)


def create_table_inventory():
    location = f"{ICEBERG_WAREHOUSE}{OPS_DB}/table_inventory/"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{OPS_DB}.table_inventory (
            subscription_name      STRING   COMMENT 'Full GCP subscription resource path',
            topic_name             STRING   COMMENT 'Short name used in table naming and Step Functions',
            raw_database           STRING,
            raw_table              STRING,
            standardized_database  STRING,
            standardized_table     STRING,
            curated_database       STRING,
            curated_table          STRING,
            is_active              BOOLEAN  COMMENT 'Include in compaction and processing when true',
            auto_ingested_on       TIMESTAMP COMMENT 'When discovery pipeline first saw this subscription',
            intake_completed_on    TIMESTAMP COMMENT 'When full pipeline was validated and marked live by Ops'
        )
        USING iceberg
        LOCATION '{location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """)
    logger.info("✅ table_inventory ready.")


def create_maintenance_log():
    location = f"{ICEBERG_WAREHOUSE}{OPS_DB}/maintenance_log/"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{OPS_DB}.maintenance_log (
            run_id          STRING    COMMENT 'UUID identifying the overall compaction job run',
            run_ts          TIMESTAMP COMMENT 'When this entry was written',
            database_name   STRING,
            table_name      STRING,
            layer           STRING    COMMENT 'RAW, STANDARDIZED, or CURATED',
            partition_date  STRING    COMMENT 'Date of the partition compacted (YYYY-MM-DD)',
            files_before    BIGINT    COMMENT 'Small file count before compaction',
            files_after     BIGINT    COMMENT 'File count after compaction',
            status          STRING    COMMENT 'SUCCESS, SKIPPED, or FAILED',
            error_message   STRING,
            duration_ms     BIGINT
        )
        USING iceberg
        PARTITIONED BY (days(run_ts))
        LOCATION '{location}'
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """)
    logger.info("✅ maintenance_log ready.")


def main():
    logger.info("=== Setting up Operational Data Store ===")
    create_database()
    create_table_inventory()
    create_maintenance_log()
    logger.info("=== Setup complete ===")
    job.commit()


if __name__ == "__main__":
    main()
