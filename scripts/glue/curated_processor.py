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
ICEBERG_WAREHOUSE = f"s3://{ICEBERG_BUCKET}/"

# Configure Iceberg catalog (required for Glue Spark)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Full table paths (must use glue_catalog prefix!)
RAW_TABLE = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
CURATED_TABLE = f"glue_catalog.{CURATED_DATABASE}.events"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_last_checkpoint(topic_name):
    """Get last processed ingestion_ts from DynamoDB checkpoint table."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    try:
        # Key structure: pipeline_id (hash) + checkpoint_type (range)
        response = table.get_item(Key={
            'pipeline_id': f'curation_{topic_name}',
            'checkpoint_type': 'curated'
        })
        if 'Item' in response:
            return int(response['Item'].get('last_ingestion_ts', 0))
    except Exception as e:
        print(f"Error getting checkpoint: {e}")
    
    return 0


def update_checkpoint(topic_name, checkpoint_value):
    """Update checkpoint in DynamoDB."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    # Key structure: pipeline_id (hash) + checkpoint_type (range)
    table.put_item(Item={
        'pipeline_id': f'curation_{topic_name}',
        'checkpoint_type': 'curated',
        'last_ingestion_ts': checkpoint_value,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    print(f"Checkpoint updated: curation_{topic_name}/curated -> {checkpoint_value}")


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
    SCHEMA EVOLUTION: Add new columns from DataFrame to Curated table.
    All new columns are added as STRING for maximum flexibility.
    
    Returns: List of newly added columns
    """
    # Get current curated table columns
    try:
        curated_cols = set(col.lower() for col in spark.table(curated_table).columns)
    except Exception as e:
        print(f"Could not get curated table columns: {e}")
        return []
    
    # Get new dataframe columns (lowercase for comparison)
    df_cols = set(col.lower() for col in df.columns)
    
    # Find new columns not in curated table
    new_cols = df_cols - curated_cols
    added_cols = []
    
    # Limit to prevent massive schema explosion
    MAX_NEW_COLS_PER_RUN = 50
    if len(new_cols) > MAX_NEW_COLS_PER_RUN:
        print(f"WARNING: {len(new_cols)} new columns detected, limiting to {MAX_NEW_COLS_PER_RUN}")
        new_cols = list(new_cols)[:MAX_NEW_COLS_PER_RUN]
    
    # Add missing columns as STRING
    for col in new_cols:
        print(f"SCHEMA EVOLUTION: Adding new column '{col}' as STRING")
        try:
            spark.sql(f"ALTER TABLE {curated_table} ADD COLUMNS ({col} STRING)")
            added_cols.append(col)
        except Exception as e:
            # Column may already exist (race condition) - that's OK
            if "already exists" in str(e).lower():
                print(f"Column '{col}' already exists, skipping")
            else:
                print(f"Error adding column '{col}': {e}")
    
    if added_cols:
        print(f"SCHEMA EVOLUTION: Added {len(added_cols)} new columns: {added_cols}")
    
    return added_cols


def align_dataframe_to_table(df, curated_table):
    """
    SCHEMA EVOLUTION: Align DataFrame columns to match Curated table schema.
    - Columns in table but missing in DF: Add as NULL
    - Columns in DF but not in table: Already handled by add_missing_columns_to_curated
    
    Returns: DataFrame with columns matching table schema
    """
    try:
        table_cols = spark.table(curated_table).columns
    except Exception as e:
        print(f"Could not get table schema for alignment: {e}")
        return df
    
    # Add missing columns as NULL
    for col in table_cols:
        if col not in df.columns:
            # Case-insensitive check
            matching_col = next((c for c in df.columns if c.lower() == col.lower()), None)
            if matching_col:
                # Rename to match case
                df = df.withColumnRenamed(matching_col, col)
            else:
                # Column is missing in data - add as NULL
                print(f"SCHEMA EVOLUTION: Column '{col}' missing in data, inserting NULL")
                df = df.withColumn(col, F.lit(None).cast(StringType()))
    
    # Reorder columns to match table schema exactly
    df_aligned = df.select(table_cols)
    
    return df_aligned


def safe_cast_to_string(df):
    """
    SCHEMA EVOLUTION: Safely cast all non-envelope columns to STRING.
    Handles: INT, LONG, DOUBLE, TIMESTAMP, BOOLEAN, etc.
    
    Returns: DataFrame with all payload columns as STRING
    """
    # Envelope columns that have specific types
    preserve_types = {'ingestion_ts'}  # Keep as BIGINT
    
    for field in df.schema.fields:
        col_name = field.name
        col_type = str(field.dataType)
        
        # Skip columns that should preserve type
        if col_name in preserve_types:
            continue
        
        # Cast to STRING if not already
        if col_type != 'StringType':
            print(f"SAFE CAST: Converting '{col_name}' from {col_type} to STRING")
            df = df.withColumn(col_name, F.col(col_name).cast(StringType()))
    
    return df


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
    
    # SCHEMA EVOLUTION Step 1: Safe cast all columns to STRING
    df_flattened = safe_cast_to_string(df_flattened)
    
    # SCHEMA EVOLUTION Step 2: Add new columns to Curated table
    new_cols = add_missing_columns_to_curated(df_flattened, CURATED_TABLE)
    if new_cols:
        print(f"Schema evolved: {len(new_cols)} new columns added")
    
    # SCHEMA EVOLUTION Step 3: Align DataFrame to table schema (handle missing columns)
    df_aligned = align_dataframe_to_table(df_flattened, CURATED_TABLE)
    print(f"Aligned columns: {df_aligned.columns}")
    
    # Check if this is first run (Curated table is empty)
    is_first_run = True
    try:
        snapshots_df = spark.sql(f"SELECT * FROM {CURATED_TABLE}.snapshots LIMIT 1")
        if snapshots_df.count() > 0:
            is_first_run = False
            print("Curated table has snapshots")
    except Exception as e:
        print(f"First run detected (no snapshots): {e}")
    
    if is_first_run:
        # First run: Use writeTo() - table should already exist from ensure_curated_table Lambda
        # But if it doesn't, this creates it
        print("First run - using writeTo() for initial load")
        df_aligned.writeTo(CURATED_TABLE).using("iceberg").createOrReplace()
        print("Initial data loaded successfully")
    else:
        # Subsequent runs: Use MERGE for LIFO dedup on idempotency_key
        # Requires IcebergSparkSessionExtensions in --conf
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
        
        print(f"Executing MERGE with {len(columns)} columns...")
        print(f"MERGE SQL: {merge_sql[:200]}...")
        spark.sql(merge_sql)
        print("MERGE complete")
    
    # Update checkpoint
    update_checkpoint(TOPIC_NAME, max_ingestion_ts)
    
    print(f"Curated processing complete. Processed {record_count} records.")
    
    job.commit()


if __name__ == "__main__":
    main()
