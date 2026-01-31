"""
==============================================================================
STANDARDIZED LAYER PROCESSOR
==============================================================================
What does this script do? (ELI5)
--------------------------------
Think of this as a "Mail Sorter":
1. Opens the mailbag (reads RAW table)
2. Throws away duplicate letters (FIFO dedup - keep first, ignore retries)
3. Unpacks envelopes (flattens nested JSON into flat columns)
4. Stamps with tracking number (adds standard column names)
5. Puts in sorted bins (writes to Standardized table)

Run by: AWS Glue (Spark job)
Input:  iceberg_raw_db.{topic}_staging
Output: iceberg_standardized_db.events

Key Concepts:
- FIFO dedup: If Lambda sends the same message twice (retry), we keep the FIRST one
- Snapshot reads: We read only NEW data since last run (like a bookmark in a book)
- Schema evolution: If new columns appear, we add them automatically
==============================================================================
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

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS
from utils.flatten import flatten_json_payload
from utils.schema_evolution import (
    add_missing_columns_to_table,
    align_dataframe_to_table,
    safe_cast_to_string,
)

logger = get_logger(__name__)


# ==============================================================================
# CONFIGURATION - These come from Step Functions when the job is triggered
# ==============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',           # Glue job name (for logging)
    'topic_name',         # Which topic to process (e.g., "events")
    'raw_database',       # Source database (iceberg_raw_db)
    'standardized_database',  # Target database (iceberg_standardized_db)
    'checkpoint_table',   # DynamoDB table to store progress
    'iceberg_bucket'      # S3 bucket where Iceberg data lives
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Store config in easy-to-read variables
TOPIC_NAME = args['topic_name']
RAW_DATABASE = args['raw_database']
STANDARDIZED_DATABASE = args['standardized_database']
CHECKPOINT_TABLE = args['checkpoint_table']
ICEBERG_BUCKET = args['iceberg_bucket']
ICEBERG_WAREHOUSE = f"s3://{ICEBERG_BUCKET}/"

# Configure Iceberg catalog (Glue Catalog integration)
for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# Full table paths (using glue_catalog prefix for Iceberg)
RAW_TABLE = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
STANDARDIZED_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.events"


# ==============================================================================
# CHECKPOINT FUNCTIONS
# ==============================================================================
# What is a checkpoint?
# ---------------------
# Think of it like a bookmark. It tells us "I've already read up to page 50."
# Next time, we start from page 51 instead of page 1.
#
# We use Iceberg "snapshot IDs" as bookmarks. Each time data is written,
# Iceberg creates a new snapshot with a unique ID.
# ==============================================================================

def get_last_snapshot_checkpoint(topic_name: str) -> str:
    """
    Get the last processed snapshot ID from DynamoDB.
    
    Returns:
        Snapshot ID as string, or "0" if this is the first run
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
            snapshot_id = response['Item'].get('last_snapshot_id', "0")
            ingestion_ts = response['Item'].get('last_ingestion_ts', 0)
            logger.info("Checkpoint found: snapshot=%s, ingestion_ts=%d", snapshot_id, ingestion_ts)
            return str(snapshot_id)
    except Exception as e:
        logger.error("Error getting checkpoint: %s", e)
    
    return "0"  # First run


def get_current_snapshot_id(table_path: str) -> str:
    """
    Get the latest snapshot ID of an Iceberg table.
    
    Think of this as asking: "What's the latest page number in the book?"
    """
    try:
        snapshots_df = spark.sql(
            f"SELECT snapshot_id FROM {table_path}.snapshots ORDER BY committed_at DESC LIMIT 1"
        )
        if snapshots_df.count() > 0:
            return str(snapshots_df.first()['snapshot_id'])
    except Exception as e:
        logger.warning("Could not get snapshot ID for %s: %s", table_path, e)
    
    return "0"


def read_incremental_from_raw(table_path: str, start_snapshot: str, end_snapshot: str):
    """
    Read only NEW data from RAW table (between two snapshots).
    
    This is like saying: "Give me pages 51-75" instead of "Give me the whole book."
    Iceberg handles this automatically with time-travel.
    
    Args:
        start_snapshot: Where we left off last time (exclusive)
        end_snapshot: Current latest snapshot (inclusive)
    
    Returns:
        DataFrame with new records, or None if no new data
    """
    if start_snapshot == "0" or start_snapshot is None:
        # First run - read everything
        logger.info("First run: Reading entire table")
        return spark.read.format("iceberg").load(table_path)
    
    if end_snapshot == start_snapshot:
        # No new data since last run
        logger.info("No new snapshots - nothing to process")
        return None
    
    # Read only the new data between snapshots
    logger.info("Reading incremental data: (%s, %s]", start_snapshot, end_snapshot)
    
    return spark.read.format("iceberg") \
        .option("start-snapshot-id", start_snapshot) \
        .option("end-snapshot-id", end_snapshot) \
        .load(table_path)


def update_checkpoint(topic_name: str, snapshot_id: str, max_ingestion_ts: int) -> None:
    """
    Save our progress to DynamoDB.
    
    Like putting a bookmark in the book: "I finished reading up to snapshot X."
    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    table.put_item(Item={
        'pipeline_id': f'standardization_{topic_name}',
        'checkpoint_type': 'standardized',
        'last_snapshot_id': snapshot_id,
        'last_ingestion_ts': max_ingestion_ts,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    logger.info("Bookmark saved: snapshot=%s", snapshot_id)


# ==============================================================================
# DEDUPLICATION
# ==============================================================================
# Why do we need this?
# --------------------
# Sometimes Lambda retries sending the same message (network hiccup).
# We don't want two copies of the same thing in our table.
#
# Solution: Keep the FIRST one, throw away duplicates.
# (FIFO = First In, First Out)
# ==============================================================================

def dedup_stage1_fifo(df):
    """
    Remove duplicate messages by keeping the FIRST occurrence.
    
    How it works:
    1. Group by message_id
    2. Sort by ingestion_ts (oldest first)
    3. Keep only row #1 in each group
    
    Example:
        Input:  [msg1 at 10:00, msg1 at 10:01, msg2 at 10:02]
        Output: [msg1 at 10:00, msg2 at 10:02]  (second msg1 removed)
    """
    # Create a window: partition by message_id, order by time (oldest first)
    window = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    
    # Add row numbers within each group
    df_with_rn = df.withColumn("rn", F.row_number().over(window))
    
    # Keep only row 1 (the first occurrence)
    df_deduped = df_with_rn.filter(F.col("rn") == 1).drop("rn")
    
    # Log how many duplicates we removed
    count_before = df.count()
    count_after = df_deduped.count()
    logger.info("Dedup: %d → %d records (%d duplicates removed)", 
                count_before, count_after, count_before - count_after)
    
    return df_deduped


# ==============================================================================
# MAIN PROCESSING FLOW
# ==============================================================================

def main():
    """
    Main entry point. Runs through these stages:
    
    1. INIT       - Get checkpoint (where did we leave off?)
    2. READ       - Read new data from RAW table
    3. VALIDATE   - Check for bad JSON, split into good/bad
    4. FLATTEN    - Unpack nested JSON into flat columns
    5. EVOLVE     - Add any new columns to the table
    6. WRITE      - Save to Standardized table
    7. CHECKPOINT - Save our progress
    """
    current_stage = "INIT"
    records_in = 0
    records_valid = 0
    records_error = 0
    records_written = 0
    
    try:
        logger.info("=== Starting Standardized Processing ===")
        logger.info("Topic: %s", TOPIC_NAME)
        
        # ===== STAGE 1: INIT =====
        # Get our bookmark (last processed snapshot)
        current_stage = "INIT"
        last_snapshot = get_last_snapshot_checkpoint(TOPIC_NAME)
        current_snapshot = get_current_snapshot_id(RAW_TABLE)
        
        logger.info("Last snapshot: %s, Current snapshot: %s", last_snapshot, current_snapshot)
        
        if current_snapshot == "0":
            logger.info("RAW table is empty - nothing to do")
            job.commit()
            return
        
        # ===== STAGE 2: READ =====
        # Read new data since last checkpoint
        current_stage = "READ"
        raw_df = read_incremental_from_raw(RAW_TABLE, last_snapshot, current_snapshot)
        
        if raw_df is None:
            logger.info("No new data - exiting")
            job.commit()
            return
        
        records_in = raw_df.count()
        logger.info("Records to process: %d", records_in)
        
        if records_in == 0:
            update_checkpoint(TOPIC_NAME, current_snapshot, 0)
            job.commit()
            return
        
        max_ingestion_ts = raw_df.agg(F.max("ingestion_ts")).collect()[0][0]
        max_ingestion_ts = int(max_ingestion_ts) if max_ingestion_ts else 0
        
        # ===== STAGE 3: VALIDATE =====
        # Remove duplicates and check for bad JSON
        current_stage = "VALIDATE"
        df_deduped = dedup_stage1_fifo(raw_df)
        
        # Check if JSON looks valid (starts/ends with {} or [])
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
        
        # Send bad records to parse_errors table
        if records_error > 0:
            logger.warn("Found %d invalid records - sending to parse_errors", records_error)
            error_df = df_invalid.select(
                F.col("json_payload").alias("raw_payload"),
                F.lit("INVALID_JSON").alias("error_type"),
                F.lit("JSON validation failed").alias("error_message"),
                F.current_timestamp().alias("processed_ts")
            )
            
            PARSE_ERRORS_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.parse_errors"
            try:
                error_df.writeTo(PARSE_ERRORS_TABLE).append()
                logger.info("Wrote %d errors to %s", records_error, PARSE_ERRORS_TABLE)
            except Exception as e:
                logger.error("Failed to write errors (continuing anyway): %s", e)
        
        if records_valid == 0:
            logger.info("No valid records - updating checkpoint and exiting")
            update_checkpoint(TOPIC_NAME, current_snapshot, max_ingestion_ts)
            job.commit()
            return
        
        # ===== STAGE 4: FLATTEN =====
        # Unpack nested JSON into flat columns
        # e.g., {"event": {"userId": "123"}} → event_userid = "123"
        current_stage = "FLATTEN"
        df_flattened = flatten_json_payload(df_valid)
        logger.info("Columns after flattening: %d", len(df_flattened.columns))
        
        # Map internal column names to standard names
        column_mappings = {
            "_metadata_idempotencykeyresource": "idempotency_key",
            "_metadata_periodreference": "period_reference",
            "_metadata_correlationid": "correlation_id",
        }
        
        for source_col, target_col in column_mappings.items():
            if source_col in df_flattened.columns:
                if target_col not in df_flattened.columns:
                    df_flattened = df_flattened.withColumn(target_col, F.col(source_col))
                    logger.info("Mapped %s → %s", source_col, target_col)
                else:
                    df_flattened = df_flattened.withColumn(
                        target_col, 
                        F.coalesce(F.col(target_col), F.col(source_col))
                    )
        
        # ===== STAGE 5: EVOLVE =====
        # Add any new columns to the table (schema evolution)
        current_stage = "EVOLVE"
        df_flattened = safe_cast_to_string(df_flattened)
        new_cols = add_missing_columns_to_table(spark, df_flattened, STANDARDIZED_TABLE)
        if new_cols:
            logger.info("Added %d new columns to table", len(new_cols))
        df_aligned = align_dataframe_to_table(spark, df_flattened, STANDARDIZED_TABLE)
        
        # ===== STAGE 6: WRITE =====
        # Save to Standardized table using MERGE (upsert)
        current_stage = "WRITE"
        is_first_run = True
        try:
            snapshots_df = spark.sql(f"SELECT * FROM {STANDARDIZED_TABLE}.snapshots LIMIT 1")
            if snapshots_df.count() > 0:
                is_first_run = False
        except Exception:
            pass
        
        if is_first_run:
            logger.info("First run - creating table with initial data")
            df_aligned.writeTo(STANDARDIZED_TABLE).using("iceberg").createOrReplace()
            records_written = df_aligned.count()
        else:
            # MERGE: Update existing rows or insert new ones
            df_aligned.createOrReplaceTempView("staged_data")
            columns = df_aligned.columns
            update_clause = ", ".join([f"t.{c} = s.{c}" for c in columns if c != 'idempotency_key'])
            insert_cols = ", ".join(columns)
            insert_vals = ", ".join([f"s.{c}" for c in columns])
            
            merge_sql = f"""
            MERGE INTO {STANDARDIZED_TABLE} t
            USING staged_data s
            ON t.idempotency_key = s.idempotency_key
               AND (
                   t.period_reference = s.period_reference
                   OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'), -1), 'yyyy-MM')
                   OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'), 1), 'yyyy-MM')
               )
            WHEN MATCHED AND s.publish_time > t.publish_time THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals})
            """
            
            logger.info("Executing MERGE...")
            spark.sql(merge_sql)
            records_written = records_valid
        
        logger.info("Write complete: %d records", records_written)
        
        # ===== STAGE 7: CHECKPOINT =====
        # Save our progress
        current_stage = "CHECKPOINT"
        update_checkpoint(TOPIC_NAME, current_snapshot, max_ingestion_ts)
        
        # Print summary
        final_count = spark.table(STANDARDIZED_TABLE).count()
        logger.info("=== SUMMARY ===")
        logger.info("IN: %d | VALID: %d | ERRORS: %d | WRITTEN: %d | TOTAL: %d", 
                    records_in, records_valid, records_error, records_written, final_count)
        
        job.commit()
        logger.info("Job completed successfully!")
        
    except Exception as e:
        logger.error("=== JOB FAILED at stage: %s ===", current_stage)
        logger.error("Error: %s", str(e))
        raise


if __name__ == "__main__":
    main()
