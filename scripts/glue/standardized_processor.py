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


def main():
    """
    Main entry point for Standardized processing.
    
    Stages:
        1. INIT: Get checkpoint
        2. READ: Read RAW data
        3. VALIDATE: JSON validation, route errors
        4. FLATTEN: Deep flatten JSON
        5. EVOLVE: Schema evolution
        6. WRITE: MERGE to Standardized table
        7. CHECKPOINT: Update DynamoDB
    """
    current_stage = "INIT"
    records_in = 0
    records_valid = 0
    records_error = 0
    records_written = 0
    
    try:
        logger.info("Starting Standardized processing for topic: %s", TOPIC_NAME)
        
        # === STAGE 1: INIT ===
        current_stage = "INIT"
        last_checkpoint = get_last_checkpoint(TOPIC_NAME)
        logger.info("Last checkpoint: %d", last_checkpoint)
        
        # === STAGE 2: READ ===
        current_stage = "READ"
        raw_df = spark.table(RAW_TABLE).filter(F.col("ingestion_ts") > last_checkpoint)
        records_in = raw_df.count()
        logger.info("Records to process: %d", records_in)
        
        if records_in == 0:
            logger.info("No new records to process - exiting gracefully")
            job.commit()
            return
        
        max_ingestion_ts = raw_df.agg(F.max("ingestion_ts")).collect()[0][0]
        logger.info("Max ingestion_ts: %d", max_ingestion_ts)
        
        # === STAGE 3: VALIDATE ===
        current_stage = "VALIDATE"
        df_deduped = dedup_stage1_fifo(raw_df)
        
        # JSON validation using native Spark SQL
        df_checked = df_deduped.withColumn(
            "_trimmed_payload",
            F.trim(F.col("json_payload"))
        ).withColumn(
            "is_valid_json",
            F.when(
                (F.col("json_payload").isNotNull()) &
                (F.col("_trimmed_payload") != "") &
                (
                    (F.col("_trimmed_payload").startswith("{") & F.col("_trimmed_payload").endswith("}")) |
                    (F.col("_trimmed_payload").startswith("[") & F.col("_trimmed_payload").endswith("]"))
                ),
                F.lit(True)
            ).otherwise(F.lit(False))
        ).drop("_trimmed_payload")
        
        df_valid = df_checked.filter(F.col("is_valid_json") == True).drop("is_valid_json")
        df_invalid = df_checked.filter(F.col("is_valid_json") == False)
        records_error = df_invalid.count()
        records_valid = df_valid.count()
        
        # Route invalid records to parse_errors
        if records_error > 0:
            logger.warn("Found %d invalid records. Routing to parse_errors.", records_error)
            error_df = df_invalid.select(
                F.col("json_payload").alias("raw_payload"),
                F.lit("INVALID_JSON").alias("error_type"),
                F.lit("JSON validation failed").alias("error_message"),
                F.current_timestamp().alias("processed_ts")
            )
            
            PARSE_ERRORS_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.parse_errors"
            try:
                error_df.writeTo(PARSE_ERRORS_TABLE).append()
                logger.info("Wrote %d invalid records to %s", records_error, PARSE_ERRORS_TABLE)
            except Exception as e:
                logger.error("Failed to write to parse_errors (non-fatal): %s", e)
        
        if records_valid == 0:
            logger.info("No valid records after validation - updating checkpoint and exiting")
            update_checkpoint(TOPIC_NAME, max_ingestion_ts)
            job.commit()
            return
        
        # === STAGE 4: FLATTEN ===
        current_stage = "FLATTEN"
        df_flattened = flatten_json_payload(df_valid)
        logger.info("Columns after flattening: %d", len(df_flattened.columns))
        
        # === STAGE 5: EVOLVE ===
        current_stage = "EVOLVE"
        df_flattened = safe_cast_to_string(df_flattened)
        new_cols = add_missing_columns_to_table(spark, df_flattened, STANDARDIZED_TABLE)
        if new_cols:
            logger.info("Schema evolved: %d new columns added", len(new_cols))
        df_aligned = align_dataframe_to_table(spark, df_flattened, STANDARDIZED_TABLE)
        
        # === STAGE 6: WRITE ===
        current_stage = "WRITE"
        is_first_run = True
        try:
            snapshots_df = spark.sql(f"SELECT * FROM {STANDARDIZED_TABLE}.snapshots LIMIT 1")
            if snapshots_df.count() > 0:
                is_first_run = False
        except Exception:
            pass  # First run
        
        if is_first_run:
            logger.info("First run - using writeTo() for initial load")
            df_aligned.writeTo(STANDARDIZED_TABLE).using("iceberg").createOrReplace()
            records_written = df_aligned.count()
        else:
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
            spark.sql(merge_sql)
            records_written = records_valid
        logger.info("WRITE complete: %d records", records_written)
        
        # === STAGE 7: CHECKPOINT ===
        current_stage = "CHECKPOINT"
        update_checkpoint(TOPIC_NAME, max_ingestion_ts)
        
        # Final summary
        final_count = spark.table(STANDARDIZED_TABLE).count()
        logger.info("=== PROCESSING SUMMARY ===")
        logger.info("Records IN: %d", records_in)
        logger.info("Records VALID: %d", records_valid)
        logger.info("Records ERROR: %d", records_error)
        logger.info("Records WRITTEN: %d", records_written)
        logger.info("Table TOTAL: %d", final_count)
        logger.info("Accountability: %d of %d (%.1f%%)", 
                   records_valid + records_error, records_in,
                   100.0 * (records_valid + records_error) / records_in if records_in > 0 else 0)
        
        job.commit()
        logger.info("Job completed successfully")
        
    except Exception as e:
        logger.error("=== JOB FAILED at stage: %s ===", current_stage)
        logger.error("Error: %s", str(e))
        logger.error("Records processed before failure: IN=%d, VALID=%d, ERROR=%d",
                    records_in, records_valid, records_error)
        raise  # Re-raise to fail the job


if __name__ == "__main__":
    main()

