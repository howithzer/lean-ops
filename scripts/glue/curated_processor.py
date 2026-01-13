"""
Curated Layer Processor - PySpark Glue Job
=============================================
RAW → Curated processing with:
- Dynamic schema flattening (ALL STRING)
- Two-stage deduplication (FIFO on message_id, LIFO on idempotency_key)
- Incremental processing via checkpoint
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
from pyspark.sql.types import StringType, MapType


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

RAW_TABLE = f"{RAW_DATABASE}.{TOPIC_NAME}_staging"
CURATED_TABLE = f"{CURATED_DATABASE}.events"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_last_checkpoint(topic_name):
    """Get last processed ingestion_ts from DynamoDB checkpoint table."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    try:
        response = table.get_item(Key={'topic_name': topic_name})
        if 'Item' in response:
            return int(response['Item'].get('curated_checkpoint', 0))
    except Exception as e:
        print(f"Error getting checkpoint: {e}")
    
    return 0


def update_checkpoint(topic_name, checkpoint_value):
    """Update checkpoint in DynamoDB."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    table.put_item(Item={
        'topic_name': topic_name,
        'curated_checkpoint': checkpoint_value,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    print(f"Checkpoint updated: {topic_name} -> {checkpoint_value}")


def flatten_json_payload(df):
    """
    Flatten json_payload column into individual STRING columns.
    Extracts all keys from JSON and creates a column for each.
    """
    # Parse JSON payload into map
    df_with_map = df.withColumn(
        "payload_map",
        F.from_json(F.col("json_payload"), MapType(StringType(), StringType()))
    )
    
    # Get all unique keys from the payload across all records
    all_keys = df_with_map.select(
        F.explode(F.map_keys(F.col("payload_map")))
    ).distinct().collect()
    
    key_list = [row[0] for row in all_keys if row[0]]
    
    # Add each key as a column
    for key in key_list:
        safe_key = key.replace("-", "_").replace(".", "_").lower()
        df_with_map = df_with_map.withColumn(
            safe_key,
            F.col("payload_map").getItem(key).cast(StringType())
        )
    
    # Drop the temporary map column and original json_payload
    df_flattened = df_with_map.drop("payload_map", "json_payload")
    
    return df_flattened


def add_missing_columns_to_curated(df, curated_table):
    """
    Check if any columns in df are missing from curated table.
    If so, ALTER TABLE ADD COLUMNS.
    """
    # Get current curated table columns
    try:
        curated_cols = set(spark.table(curated_table).columns)
    except:
        curated_cols = set()
    
    # Get new dataframe columns
    df_cols = set(df.columns)
    
    # Find missing columns
    new_cols = df_cols - curated_cols
    
    # Add missing columns
    for col in new_cols:
        if col not in ['message_id', 'idempotency_key', 'ingestion_ts']:  # Skip core cols
            print(f"Adding new column to Curated table: {col}")
            try:
                spark.sql(f"ALTER TABLE {curated_table} ADD COLUMNS ({col} STRING)")
            except Exception as e:
                print(f"Column {col} may already exist or error: {e}")


def dedup_stage1_fifo(df):
    """
    Stage 1: Remove network duplicates (FIFO on message_id).
    Keep first occurrence of each message_id.
    """
    window = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    
    df_with_rn = df.withColumn("rn", F.row_number().over(window))
    df_deduped = df_with_rn.filter(F.col("rn") == 1).drop("rn")
    
    return df_deduped


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def main():
    print(f"Starting Curated processing for topic: {TOPIC_NAME}")
    
    # Get last checkpoint
    last_checkpoint = get_last_checkpoint(TOPIC_NAME)
    print(f"Last checkpoint: {last_checkpoint}")
    
    # Read RAW data incrementally
    raw_df = spark.table(RAW_TABLE).filter(F.col("ingestion_ts") > last_checkpoint)
    
    record_count = raw_df.count()
    print(f"Records to process: {record_count}")
    
    if record_count == 0:
        print("No new records to process")
        job.commit()
        return
    
    # Get max ingestion_ts for new checkpoint
    max_ingestion_ts = raw_df.agg(F.max("ingestion_ts")).collect()[0][0]
    print(f"Max ingestion_ts: {max_ingestion_ts}")
    
    # Stage 1: FIFO dedup on message_id (remove network duplicates)
    df_deduped = dedup_stage1_fifo(raw_df)
    print(f"After FIFO dedup: {df_deduped.count()} records")
    
    # Flatten JSON payload to STRING columns
    df_flattened = flatten_json_payload(df_deduped)
    print(f"Columns after flattening: {df_flattened.columns}")
    
    # Check for schema evolution (new columns)
    add_missing_columns_to_curated(df_flattened, CURATED_TABLE)
    
    # Stage 2: MERGE into Curated (LIFO on idempotency_key)
    # Create temp view for merge
    df_flattened.createOrReplaceTempView("staged_data")
    
    # Get column list for merge
    columns = df_flattened.columns
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
    
    print(f"Executing MERGE...")
    spark.sql(merge_sql)
    
    # Update checkpoint
    update_checkpoint(TOPIC_NAME, max_ingestion_ts)
    
    print(f"Curated processing complete. Processed {record_count} records.")
    
    job.commit()


if __name__ == "__main__":
    main()
