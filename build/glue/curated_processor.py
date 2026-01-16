"""
Curated Layer Processor - PySpark Glue Job
=============================================
RAW → Curated processing with:
- Dynamic schema flattening (ALL STRING)
- Two-stage deduplication (FIFO on message_id, LIFO on idempotency_key)
- Incremental processing via checkpoint
- Schema evolution (column addition, NULL handling)

Author: lean-ops team
Version: 2.0.0 (refactored)
"""

import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Import refactored utilities
from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS
from utils.flatten import flatten_json_payload
from utils.schema_evolution import (
    add_missing_columns_to_table,
    align_dataframe_to_table,
    safe_cast_to_string,
)

logger = get_logger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'topic_name',
    'raw_database',
    'curated_database',
    'checkpoint_table',
    'iceberg_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TOPIC_NAME = args['topic_name']
RAW_DATABASE = args['raw_database']
CURATED_DATABASE = args['curated_database']
CHECKPOINT_TABLE = args['checkpoint_table']
ICEBERG_BUCKET = args['iceberg_bucket']
ICEBERG_WAREHOUSE = f"s3://{ICEBERG_BUCKET}/"

# Configure Iceberg catalog
for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# Full table paths (must use glue_catalog prefix!)
RAW_TABLE = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
CURATED_TABLE = f"glue_catalog.{CURATED_DATABASE}.events"


# =============================================================================
# CHECKPOINT FUNCTIONS
# =============================================================================

def get_last_checkpoint(topic_name: str) -> int:
    """
    Get last processed ingestion_ts from DynamoDB checkpoint table.
    
    Args:
        topic_name: Topic identifier for the checkpoint
    
    Returns:
        Last processed ingestion_ts as integer (0 if not found)
    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    try:
        response = table.get_item(Key={
            'pipeline_id': f'curation_{topic_name}',
            'checkpoint_type': 'curated'
        })
        if 'Item' in response:
            checkpoint = int(response['Item'].get('last_ingestion_ts', 0))
            logger.info("Retrieved checkpoint for %s: %d", topic_name, checkpoint)
            return checkpoint
    except Exception as e:
        logger.error("Error getting checkpoint: %s", e)
    
    return 0


def update_checkpoint(topic_name: str, checkpoint_value: int) -> None:
    """
    Update checkpoint in DynamoDB.
    
    Args:
        topic_name: Topic identifier for the checkpoint
        checkpoint_value: New ingestion_ts value to store
    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    table.put_item(Item={
        'pipeline_id': f'curation_{topic_name}',
        'checkpoint_type': 'curated',
        'last_ingestion_ts': checkpoint_value,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    logger.info("Checkpoint updated: curation_%s/curated -> %d", topic_name, checkpoint_value)


# =============================================================================
# DEDUPLICATION FUNCTIONS
# =============================================================================

def dedup_stage1_fifo(df):
    """
    Stage 1 Deduplication: FIFO on message_id.
    
    Keeps the FIRST occurrence of each message_id (by ingestion_ts ASC).
    This removes network duplicates from Lambda retries.
    
    Args:
        df: Raw DataFrame with message_id and ingestion_ts columns
    
    Returns:
        DataFrame with duplicates removed
    """
    window = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    
    df_with_rn = df.withColumn("rn", F.row_number().over(window))
    df_deduped = df_with_rn.filter(F.col("rn") == 1).drop("rn")
    
    count_before = df.count()
    count_after = df_deduped.count()
    logger.info("FIFO dedup: %d -> %d records (removed %d duplicates)", 
                count_before, count_after, count_before - count_after)
    
    return df_deduped


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def main():
    """Main entry point for Curated processing."""
    logger.info("Starting Curated processing for topic: %s", TOPIC_NAME)
    
    # Get last checkpoint
    last_checkpoint = get_last_checkpoint(TOPIC_NAME)
    logger.info("Last checkpoint: %d", last_checkpoint)
    
    # Read RAW data incrementally
    raw_df = spark.table(RAW_TABLE).filter(F.col("ingestion_ts") > last_checkpoint)
    
    record_count = raw_df.count()
    logger.info("Records to process: %d", record_count)
    
    if record_count == 0:
        logger.info("No new records to process")
        job.commit()
        return
    
    # Get max ingestion_ts for new checkpoint
    max_ingestion_ts = raw_df.agg(F.max("ingestion_ts")).collect()[0][0]
    logger.info("Max ingestion_ts: %d", max_ingestion_ts)
    
    # Stage 1: FIFO dedup on message_id (remove network duplicates)
    df_deduped = dedup_stage1_fifo(raw_df)
    
    # Flatten JSON payload to STRING columns (deep flatten up to 5 levels)
    df_flattened = flatten_json_payload(df_deduped)
    logger.info("Columns after flattening: %s", df_flattened.columns)
    
    # SCHEMA EVOLUTION Step 1: Safe cast all columns to STRING
    df_flattened = safe_cast_to_string(df_flattened)
    
    # SCHEMA EVOLUTION Step 2: Add new columns to Curated table
    new_cols = add_missing_columns_to_table(spark, df_flattened, CURATED_TABLE)
    if new_cols:
        logger.info("Schema evolved: %d new columns added", len(new_cols))
    
    # SCHEMA EVOLUTION Step 3: Align DataFrame to table schema
    df_aligned = align_dataframe_to_table(spark, df_flattened, CURATED_TABLE)
    logger.info("DataFrame aligned to table schema")
    
    # Check if this is first run (Curated table is empty)
    is_first_run = True
    try:
        snapshots_df = spark.sql(f"SELECT * FROM {CURATED_TABLE}.snapshots LIMIT 1")
        if snapshots_df.count() > 0:
            is_first_run = False
            logger.info("Curated table has existing snapshots")
    except Exception as e:
        logger.info("First run detected (no snapshots): %s", e)
    
    if is_first_run:
        # First run: Use writeTo() for initial load
        logger.info("First run - using writeTo() for initial load")
        df_aligned.writeTo(CURATED_TABLE).using("iceberg").createOrReplace()
        logger.info("Initial data loaded successfully")
    else:
        # Subsequent runs: Use MERGE for LIFO dedup on idempotency_key
        df_aligned.createOrReplaceTempView("staged_data")
        
        columns = df_aligned.columns
        update_clause = ", ".join([f"t.{c} = s.{c}" for c in columns if c != 'idempotency_key'])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])
        
        merge_sql = f"""
        MERGE INTO {CURATED_TABLE} t
        USING staged_data s
        ON t.idempotency_key = s.idempotency_key
        WHEN MATCHED AND s.publish_time > t.publish_time THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        logger.info("Executing MERGE with %d columns...", len(columns))
        logger.debug("MERGE SQL: %s...", merge_sql[:200])
        spark.sql(merge_sql)
        logger.info("MERGE complete")
    
    # Update checkpoint
    update_checkpoint(TOPIC_NAME, max_ingestion_ts)
    
    # Get final count
    final_count = spark.table(CURATED_TABLE).count()
    logger.info("Curated table now has %d total records", final_count)
    
    job.commit()
    logger.info("Job completed successfully")


if __name__ == "__main__":
    main()
