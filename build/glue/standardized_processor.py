"""
Standardized Layer Processor - PySpark Glue Job
================================================
RAW → Standardized processing with:
- Dynamic schema flattening (ALL STRING)
- Two-stage deduplication (FIFO on message_id, LIFO on idempotency_key)
- Incremental processing via checkpoint
- Schema evolution (column addition, NULL handling)

Author: lean-ops team
Version: 2.1.0 (layer rename: Curated → Standardized)
"""

import sys
import json
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
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
    'standardized_database',
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
STANDARDIZED_DATABASE = args['standardized_database']
CHECKPOINT_TABLE = args['checkpoint_table']
ICEBERG_BUCKET = args['iceberg_bucket']
ICEBERG_WAREHOUSE = f"s3://{ICEBERG_BUCKET}/"

# Configure Iceberg catalog
for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# Full table paths (must use glue_catalog prefix!)
RAW_TABLE = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
STANDARDIZED_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.events"


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
            'pipeline_id': f'standardization_{topic_name}',
            'checkpoint_type': 'standardized'
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
        'pipeline_id': f'standardization_{topic_name}',
        'checkpoint_type': 'standardized',
        'last_ingestion_ts': checkpoint_value,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    logger.info("Checkpoint updated: standardization_%s/standardized -> %d", topic_name, checkpoint_value)


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

def is_valid_json(payload):
    """
    Validate if a string is valid JSON.
    This must be defined at module level for PySpark serialization.
    """
    if not payload: 
        return False
    try:
        json.loads(payload)
        return True
    except:
        return False

def main():
    """Main entry point for Standardized processing."""
    logger.info("Starting Standardized processing for topic: %s", TOPIC_NAME)
    
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

    # --- ERROR ROUTING: VALIDATE JSON ---
    is_valid_udf = F.udf(is_valid_json, BooleanType())
    
    df_checked = df_deduped.withColumn("is_valid_json", is_valid_udf(F.col("json_payload")))
    
    # Split valid/invalid
    df_valid = df_checked.filter(F.col("is_valid_json") == True).drop("is_valid_json")
    df_invalid = df_checked.filter(F.col("is_valid_json") == False)
    
    # Write invalid to parse_errors
    if df_invalid.count() > 0:
        logger.warn("Found %d invalid records. Routing to parse_errors.", df_invalid.count())
        
        # Prepare error table schema
        # DDL: raw_payload STRING, error_type STRING, error_message STRING, processed_ts TIMESTAMP
        error_df = df_invalid.select(
            F.col("json_payload").alias("raw_payload"),
            F.lit("INVALID_JSON").alias("error_type"),
            F.lit("JSON decoding failed").alias("error_message"),
            F.current_timestamp().alias("processed_ts")
        )
        
        # Write to parse_errors (Append)
        # Note: We use glue_catalog prefix
        PARSE_ERRORS_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.parse_errors"
        try:
            # Check if table exists (or just try writeTo) - Assuming logic in DDL setup
            # We use writeTo().append() for Iceberg
            error_df.writeTo(PARSE_ERRORS_TABLE).append()
            logger.info("Successfully wrote invalid records to %s", PARSE_ERRORS_TABLE)
        except Exception as e:
            logger.error("Failed to write to parse_errors: %s", e)
            
    # Proceed with valid records
    if df_valid.count() == 0:
        logger.info("No valid records to process after validation")
        update_checkpoint(TOPIC_NAME, max_ingestion_ts)
        job.commit()
        return
    
    # Flatten JSON payload to STRING columns (deep flatten up to 5 levels)
    df_flattened = flatten_json_payload(df_valid)
    logger.info("Columns after flattening: %s", df_flattened.columns)
    
    # SCHEMA EVOLUTION Step 1: Safe cast all columns to STRING
    df_flattened = safe_cast_to_string(df_flattened)
    
    # SCHEMA EVOLUTION Step 2: Add new columns to Standardized table
    new_cols = add_missing_columns_to_table(spark, df_flattened, STANDARDIZED_TABLE)
    if new_cols:
        logger.info("Schema evolved: %d new columns added", len(new_cols))
    
    # SCHEMA EVOLUTION Step 3: Align DataFrame to table schema
    df_aligned = align_dataframe_to_table(spark, df_flattened, STANDARDIZED_TABLE)
    logger.info("DataFrame aligned to table schema")
    
    # Check if this is first run (Standardized table is empty)
    is_first_run = True
    try:
        snapshots_df = spark.sql(f"SELECT * FROM {STANDARDIZED_TABLE}.snapshots LIMIT 1")
        if snapshots_df.count() > 0:
            is_first_run = False
            logger.info("Standardized table has existing snapshots")
    except Exception as e:
        logger.info("First run detected (no snapshots): %s", e)
    
    if is_first_run:
        # First run: Use writeTo() for initial load
        logger.info("First run - using writeTo() for initial load")
        df_aligned.writeTo(STANDARDIZED_TABLE).using("iceberg").createOrReplace()
        logger.info("Initial data loaded successfully")
    else:
        # Subsequent runs: Use MERGE for LIFO dedup on idempotency_key
        df_aligned.createOrReplaceTempView("staged_data")
        
        columns = df_aligned.columns
        update_clause = ", ".join([f"t.{c} = s.{c}" for c in columns if c != 'idempotency_key'])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])
        
        merge_sql = f"""
        MERGE INTO {STANDARDIZED_TABLE} t
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
    final_count = spark.table(STANDARDIZED_TABLE).count()
    logger.info("Standardized table now has %d total records", final_count)
    
    job.commit()
    logger.info("Job completed successfully")


if __name__ == "__main__":
    main()
