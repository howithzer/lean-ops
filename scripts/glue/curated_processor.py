"""
==============================================================================
CURATED LAYER PROCESSOR
==============================================================================
What does this script do? (ELI5)
--------------------------------
Think of this as a "Quality Inspector":
1. Opens the sorted mail (reads Standardized table)
2. Checks for required stamps (CDE validation - Critical Data Elements)
3. Converts types (e.g., "123" string → 123 number)
4. Rejects bad items (sends to errors table)
5. Stamps as approved (writes to Curated table)

Run by: AWS Glue (Spark job)
Input:  iceberg_standardized_db.events
Output: iceberg_curated_db.events (good) + iceberg_curated_db.errors (bad)

Key Concepts:
- CDE: "Critical Data Element" = required field that MUST exist
- LIFO dedup: If same record comes twice, keep the LATEST (business correction)
- Type casting: Convert strings to proper types (INT, TIMESTAMP, DECIMAL)
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
from pyspark.sql.types import IntegerType, TimestampType, DoubleType, DecimalType

from utils.config import get_logger, ICEBERG_CATALOG_SETTINGS

logger = get_logger(__name__)


# ==============================================================================
# CONFIGURATION - These come from Step Functions when the job is triggered
# ==============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',               # Glue job name (for logging)
    'topic_name',             # Which topic to process (e.g., "events")
    'standardized_database',  # Source database (iceberg_standardized_db)
    'curated_database',       # Target database (iceberg_curated_db)
    'checkpoint_table',       # DynamoDB table to store progress
    'iceberg_bucket',         # S3 bucket where Iceberg data lives
    'schema_bucket'           # S3 bucket where schema JSON files live
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Store config in easy-to-read variables
TOPIC_NAME = args['topic_name']
STANDARDIZED_DATABASE = args['standardized_database']
CURATED_DATABASE = args['curated_database']
CHECKPOINT_TABLE = args['checkpoint_table']
ICEBERG_BUCKET = args['iceberg_bucket']
SCHEMA_BUCKET = args['schema_bucket']
ICEBERG_WAREHOUSE = f"s3://{ICEBERG_BUCKET}/"

# Configure Iceberg catalog (Glue Catalog integration)
for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# Full table paths (using glue_catalog prefix for Iceberg)
STANDARDIZED_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.{TOPIC_NAME}"
CURATED_TABLE = f"glue_catalog.{CURATED_DATABASE}.{TOPIC_NAME}"
ERRORS_TABLE = f"glue_catalog.{CURATED_DATABASE}.errors"


# ==============================================================================
# SCHEMA LOADING
# ==============================================================================
# The schema file tells us:
# - Which columns exist
# - What type each column should be (INT, TIMESTAMP, etc.)
# - Which columns are required (CDE = Critical Data Element)
# ==============================================================================

def load_schema(bucket: str, key: str) -> dict:
    """
    Load the curated schema from S3.
    
    The schema is a JSON file that defines:
    - columns: What columns exist and their types
    - cde: Which columns are required (Critical Data Elements)
    
    Example schema:
    {
        "columns": {
            "idempotency_key": {"type": "STRING", "cde": true, "nullable": false},
            "amount": {"type": "DECIMAL(10,2)", "cde": false},
            "event_timestamp": {"type": "TIMESTAMP", "cde": true, "nullable": false}
        }
    }
    """
    import boto3
    
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        schema = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("Loaded schema from s3://%s/%s", bucket, key)
        return schema
    except Exception as e:
        logger.error("Failed to load schema: %s", e)
        raise


def create_curated_table_from_schema(spark, schema: dict, table_name: str, location: str):
    """
    Create Curated table from schema definition if it doesn't exist.
    
    This allows schema-driven table creation - the Curated table is only
    created when the schema file is deployed, not at Terraform time.
    
    Args:
        spark: SparkSession
        schema: Schema dictionary from S3
        table_name: Full table name (e.g., glue_catalog.iceberg_curated_db.events)
        location: S3 location for table data
    """
    # Check if table already exists
    db_name, tbl_name = table_name.split('.')[-2:]
    if spark.catalog.tableExists(f"{db_name}.{tbl_name}"):
        logger.info("Table %s already exists, skipping creation", table_name)
        return
    
    logger.info("Creating Curated table %s from schema...", table_name)
    
    # Build column definitions from schema
    column_defs = []
    for col_name, col_spec in schema.get('columns', {}).items():
        col_type = col_spec.get('type', 'STRING')
        # Map schema types to Spark SQL types
        if col_type.startswith('DECIMAL'):
            spark_type = col_type  # DECIMAL(10,2)
        elif col_type == 'INT':
            spark_type = 'INT'
        elif col_type == 'BIGINT':
            spark_type = 'BIGINT'
        elif col_type == 'DOUBLE':
            spark_type = 'DOUBLE'
        elif col_type == 'TIMESTAMP':
            spark_type = 'TIMESTAMP'
        else:
            spark_type = 'STRING'
        
        column_defs.append(f"{col_name} {spark_type}")
    
    # Add audit columns
    audit_cols = schema.get('audit_columns', {})
    for audit_col, spec in audit_cols.items():
        audit_type = spec.get('type', 'STRING')
        if audit_type == 'TIMESTAMP':
            column_defs.append(f"{audit_col} TIMESTAMP")
        else:
            column_defs.append(f"{audit_col} STRING")
    
    columns_sql = ",\n        ".join(column_defs)

    # Get partitioning from schema (default to period_reference if not specified)
    partition_spec = schema.get('partitioning', {})
    partition_fields = partition_spec.get('fields', ['period_reference', 'day(publish_time)'])

    # Build PARTITIONED BY clause
    # For Iceberg, use PARTITIONED BY in CREATE TABLE statement
    partitioned_by_clause = f"PARTITIONED BY ({', '.join(partition_fields)})" if partition_fields else ""

    # Build CREATE TABLE DDL with partitioning included
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    )
    USING iceberg
    {partitioned_by_clause}
    LOCATION '{location}'
    TBLPROPERTIES (
        'format-version' = '2',
        'write.format.default' = 'parquet'
    )
    """

    logger.info("Executing DDL to create table with partitioning...")
    logger.info("Partition fields: %s", partition_fields)
    spark.sql(create_sql)
    logger.info("✅ Table %s created successfully with partitions", table_name)



# ==============================================================================
# CHECKPOINT FUNCTIONS
# ==============================================================================
# Unlike Standardized (which uses snapshots), Curated uses timestamps.
# Why? Because MERGE operations invalidate snapshots, so we track
# the timestamp of the last record we processed instead.
# ==============================================================================

def get_last_checkpoint(topic_name: str) -> int:
    """
    Get the last processed timestamp from DynamoDB.
    
    Returns:
        Timestamp as epoch (e.g., 1642000000) or 0 for first run
    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    try:
        response = table.get_item(Key={
            'pipeline_id': f'curated_{topic_name}',
            'checkpoint_type': 'curated'
        })
        if 'Item' in response:
            checkpoint = int(response['Item'].get('last_ingestion_ts', 0))
            logger.info("Checkpoint found: %d", checkpoint)
            return checkpoint
    except Exception as e:
        logger.error("Error getting checkpoint: %s", e)
    
    return 0  # First run


def update_checkpoint(topic_name: str, checkpoint_value: int) -> None:
    """
    Save our progress to DynamoDB.
    
    Like putting a bookmark: "I've processed all records up to this timestamp."
    """
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    table.put_item(Item={
        'pipeline_id': f'curated_{topic_name}',
        'checkpoint_type': 'curated',
        'last_ingestion_ts': checkpoint_value,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    logger.info("Bookmark saved: timestamp=%d", checkpoint_value)


# ==============================================================================
# CDE VALIDATION
# ==============================================================================
# CDE = Critical Data Element
#
# These are columns that MUST have a value. If they're null, the record is
# rejected and sent to the errors table.
#
# Example: Every event must have an idempotency_key and event_timestamp.
# If either is missing, we can't process it reliably.
# ==============================================================================

def validate_cdes(df, schema: dict):
    """
    Check that all required fields have values.
    
    Args:
        df: DataFrame to validate
        schema: Schema defining which columns are CDEs
    
    Returns:
        (valid_df, errors_df) - Records that passed and failed validation
    """
    columns = schema.get('columns', {})
    
    # Find columns marked as CDE and not nullable
    cde_columns = [col for col, spec in columns.items() if spec.get('cde', False)]
    
    logger.info("Checking CDEs: %s", cde_columns)
    
    # Build condition: all CDEs must be non-null
    conditions = []
    for col_name in cde_columns:
        spec = columns[col_name]
        if not spec.get('nullable', True):
            conditions.append(F.col(col_name).isNotNull())
    
    if not conditions:
        # No CDE validation needed
        return df, None
    
    # Combine conditions: CDE1 is not null AND CDE2 is not null AND ...
    valid_condition = conditions[0]
    for cond in conditions[1:]:
        valid_condition = valid_condition & cond
    
    # Split into valid and invalid
    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition)
    
    # Get counts for spike detection
    total_count = df.count()
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    
    # CDE SPIKE DETECTION: Alert if violation rate exceeds 10%
    CDE_SPIKE_THRESHOLD = 0.10  # 10%
    
    if total_count > 0:
        error_rate = invalid_count / total_count
        logger.info("CDE validation: %d total, %d passed (%.2f%%), %d failed (%.2f%%)", 
                   total_count, valid_count, (valid_count/total_count)*100, 
                   invalid_count, error_rate*100)
        
        if error_rate > CDE_SPIKE_THRESHOLD:
            error_msg = (
                f"⚠️  CDE VIOLATION SPIKE DETECTED: {error_rate:.2%} error rate exceeds {CDE_SPIKE_THRESHOLD:.0%} threshold. "
                f"Total: {total_count}, Violations: {invalid_count}. "
                "This may indicate a source system data quality issue. "
                "INVESTIGATE IMMEDIATELY to prevent data loss."
            )
            logger.error(error_msg)
            
            # Log to CloudWatch Metrics for alerting
            try:
                import boto3
                from botocore.exceptions import ClientError
                
                cloudwatch = boto3.client('cloudwatch')
                cloudwatch.put_metric_data(
                    Namespace='LeanOps/DataQuality',
                    MetricData=[
                        {
                            'MetricName': 'CDE_ViolationRate',
                            'Value': error_rate * 100,  # As percentage
                            'Unit': 'Percent',
                            'Dimensions': [
                                {'Name': 'Layer', 'Value': 'Curated'},
                                {'Name': 'JobName', 'Value': 'curated_processor'}
                            ]
                        },
                        {
                            'MetricName': 'CDE_ViolationCount',
                            'Value': invalid_count,
                            'Unit': 'Count',
                            'Dimensions': [
                                {'Name': 'Layer', 'Value': 'Curated'},
                                {'Name': 'JobName', 'Value': 'curated_processor'}
                            ]
                        }
                    ]
                )
                logger.info("Published CDE spike metrics to CloudWatch")
            except (ClientError, Exception) as e:
                logger.warning("Could not publish CloudWatch metrics: %s", e)
            
            # OPTIONAL: Uncomment to FAIL the pipeline on spike detection
            # This prevents data loss but stops processing
            # raise Exception(error_msg)
    
    # Prepare error records for the errors table
    if invalid_count > 0:
        errors_df = invalid_df.select(
            F.col("message_id"),
            F.col("idempotency_key"),
            F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("raw_record"),
            F.lit("CDE_VIOLATION").alias("error_type"),
            F.lit(",".join(cde_columns)).alias("error_field"),
            F.lit("Required CDE field is null").alias("error_message"),
            F.current_timestamp().alias("processed_ts")
        )
        return valid_df, errors_df
    
    return valid_df, None


# ==============================================================================
# TYPE CASTING
# ==============================================================================
# In the Standardized layer, everything is a STRING.
# In the Curated layer, we convert to proper types:
#   - "123" → 123 (INT)
#   - "2024-01-01T00:00:00Z" → timestamp (TIMESTAMP)
#   - "99.99" → 99.99 (DECIMAL)
# ==============================================================================

def cast_types(df, schema: dict):
    """
    Convert STRING columns to their proper types based on schema.
    
    Example:
        Before: amount = "99.99" (string)
        After:  amount = 99.99 (decimal)
    """
    columns = schema.get('columns', {})
    
    for col_name, spec in columns.items():
        if col_name not in df.columns:
            continue
        
        target_type = spec.get('type', 'STRING')
        
        if target_type == 'INT':
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
        elif target_type == 'TIMESTAMP':
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
        elif target_type == 'DOUBLE':
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
        elif target_type.startswith('DECIMAL'):
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
        # STRING stays as is
    
    logger.info("Type casting complete")
    return df


# ==============================================================================
# AUDIT COLUMNS
# ==============================================================================
# We add tracking columns to every record:
# - first_seen_ts: When did we first see this record?
# - last_updated_ts: When was it last updated?
# - _schema_version: Which version of the schema was used?
# ==============================================================================

def add_audit_columns(df, schema: dict):
    """Add tracking columns for data governance."""
    now = F.current_timestamp()
    schema_version = schema.get('audit_columns', {}).get('_schema_version', {}).get('default', '1.0.0')
    
    df = df.withColumn("first_seen_ts", now)
    df = df.withColumn("last_updated_ts", now)
    df = df.withColumn("_schema_version", F.lit(schema_version))
    
    return df


# ==============================================================================
# WRITE FUNCTIONS
# ==============================================================================

def write_errors(errors_df):
    """
    Write failed records to the errors table.
    
    This lets us investigate later why certain records were rejected.
    """
    if errors_df is None or errors_df.count() == 0:
        logger.info("No errors to write")
        return
    
    error_count = errors_df.count()
    errors_df.writeTo(ERRORS_TABLE).using("iceberg").append()
    logger.info("Wrote %d error records to %s", error_count, ERRORS_TABLE)


def add_missing_columns_to_curated(df):
    """
    Add new columns to the Curated table if they don't exist.
    
    This is "schema evolution" - if new columns appear in the data,
    we automatically add them to the table structure.
    """
    try:
        # Get existing columns from target table
        target_df = spark.table(CURATED_TABLE).limit(0)  # Just schema, no data
        target_cols = set(c.lower() for c in target_df.columns)
        source_cols = set(c.lower() for c in df.columns)
        
        # Find new columns
        new_cols = source_cols - target_cols
        
        if not new_cols:
            logger.info("No new columns to add")
            return
        
        logger.info("Adding %d new columns: %s", len(new_cols), sorted(new_cols))
        
        # Add each new column to the table
        for col in sorted(new_cols):
            alter_sql = f"ALTER TABLE {CURATED_TABLE} ADD COLUMN {col} STRING"
            try:
                spark.sql(alter_sql)
                logger.info("Added column: %s", col)
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info("Column %s already exists", col)
                else:
                    logger.warning("Failed to add column %s: %s", col, e)
    
    except Exception as e:
        logger.warning("Schema evolution check failed: %s", e)


def write_to_curated(df, is_first_run: bool):
    """
    Write valid records to the Curated table using MERGE.

    IMPORTANT: Always use MERGE, even for empty tables.
    createOrReplace() loses DDL-defined partitioning and table properties.
    MERGE works on empty tables when IcebergSparkSessionExtensions is enabled.
    """
    if df.count() == 0:
        logger.info("No valid records to write")
        return

    # Schema evolution: add any new columns before MERGE
    add_missing_columns_to_curated(df)

    # MERGE = "upsert" (update if exists, insert if new)
    # Works on both empty and populated tables
    df.createOrReplaceTempView("staged_data")

    columns = df.columns
    update_clause = ", ".join([f"t.{c} = s.{c}" for c in columns if c != 'idempotency_key'])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{c}" for c in columns])

    # LIFO: Only update if the new record is NEWER than existing
    # Enhanced: Handle cross-period corrections (±1 month)
    merge_sql = f"""
    MERGE INTO {CURATED_TABLE} t
    USING staged_data s
    ON t.idempotency_key = s.idempotency_key
       AND (
           t.period_reference = s.period_reference
           OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'), -1), 'yyyy-MM')
           OR t.period_reference = date_format(add_months(to_date(s.period_reference, 'yyyy-MM'), 1), 'yyyy-MM')
       )
    WHEN MATCHED AND s.last_updated_ts > t.last_updated_ts THEN
        UPDATE SET {update_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
    """

    logger.info("Executing MERGE (works on empty tables too)...")
    spark.sql(merge_sql)
    logger.info("MERGE complete")


# ==============================================================================
# MAIN PROCESSING FLOW
# ==============================================================================

def main():
    """
    Main entry point. Runs through these stages:
    
    1. INIT           - Load schema file from S3
    2. CHECKPOINT     - Get our bookmark (where did we leave off?)
    3. READ           - Read new data from Standardized table
    4. CDE_VALIDATE   - Check for required fields
    5. CAST           - Convert types (string → int, timestamp, etc.)
    6. WRITE          - Save to Curated table
    7. UPDATE_CHECKPOINT - Save our progress
    """
    current_stage = "INIT"
    records_in = 0
    records_valid = 0
    records_error = 0
    records_written = 0
    
    try:
        logger.info("=== Starting Curated Processing ===")
        logger.info("Topic: %s", TOPIC_NAME)
        
        # ===== STAGE 1: INIT =====
        # Load the schema that defines column types and CDEs
        current_stage = "INIT"
        try:
            schema = load_schema(SCHEMA_BUCKET, "schemas/curated_schema.json")
        except Exception as e:
            logger.error("Failed to load schema: %s", e)
            raise RuntimeError(f"Schema loading failed: {e}")
        
        if not schema.get('columns'):
            raise RuntimeError("Schema missing 'columns' definition")
        logger.info("Schema loaded with %d columns", len(schema.get('columns', {})))
        
        # NOTE: Table should already be created by schema_validator Lambda.
        # This is a safety fallback for direct testing or legacy deployments.
        curated_location = f"s3://{ICEBERG_BUCKET}/iceberg_curated_db/events/"
        create_curated_table_from_schema(spark, schema, CURATED_TABLE, curated_location)
        
        # Also ensure errors and drift_log tables exist
        errors_location = f"s3://{ICEBERG_BUCKET}/iceberg_curated_db/errors/"
        drift_log_location = f"s3://{ICEBERG_BUCKET}/iceberg_curated_db/drift_log/"
        
        # Create errors table (hardcoded schema - doesn't change)
        if not spark.catalog.tableExists("iceberg_curated_db.errors"):
            logger.info("Creating Curated errors table...")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {ERRORS_TABLE} (
                    message_id STRING,
                    idempotency_key STRING,
                    raw_record STRING COMMENT 'Serialized source record',
                    error_type STRING COMMENT 'CDE_VIOLATION, TYPE_CAST_ERROR',
                    error_field STRING COMMENT 'Which field failed validation',
                    error_message STRING,
                    processed_ts TIMESTAMP
                )
                USING iceberg
                LOCATION '{errors_location}'
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.format.default' = 'parquet'
                )
            """)
            logger.info("✅ Errors table created")
        
        # Create drift_log table (hardcoded schema)
        drift_log_table = f"glue_catalog.{CURATED_DATABASE}.drift_log"
        if not spark.catalog.tableExists("iceberg_curated_db.drift_log"):
            logger.info("Creating drift_log table...")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {drift_log_table} (
                    detected_ts TIMESTAMP,
                    column_name STRING,
                    action STRING COMMENT 'ADDED, REMOVED, TYPE_CHANGED',
                    source_layer STRING COMMENT 'standardized, curated',
                    old_value STRING,
                    new_value STRING,
                    details STRING
                )
                USING iceberg
                LOCATION '{drift_log_location}'
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.format.default' = 'parquet'
                )
            """)
            logger.info("✅ drift_log table created")
        

        # ===== STAGE 2: CHECKPOINT =====
        # Get our bookmark (last processed timestamp)
        current_stage = "CHECKPOINT"
        last_checkpoint = get_last_checkpoint(TOPIC_NAME)
        logger.info("Last checkpoint: %d", last_checkpoint)
        
        # ===== STAGE 3: READ =====
        # Read new data from Standardized (only records after our checkpoint)
        current_stage = "READ"
        standardized_df = spark.table(STANDARDIZED_TABLE).filter(
            F.col("ingestion_ts") > F.lit(last_checkpoint)
        )
        records_in = standardized_df.count()
        logger.info("Records to process: %d", records_in)
        
        if records_in == 0:
            logger.info("No new records - exiting")
            job.commit()
            return
        
        max_ts = standardized_df.agg(F.max("ingestion_ts")).collect()[0][0]
        logger.info("Max timestamp: %d", max_ts if max_ts else 0)
        
        # ===== STAGE 4: CDE_VALIDATE =====
        # Check that required fields have values
        current_stage = "CDE_VALIDATE"
        valid_df, errors_df = validate_cdes(standardized_df, schema)
        records_valid = valid_df.count()
        records_error = errors_df.count() if errors_df else 0
        
        # Write rejected records to errors table
        if errors_df and records_error > 0:
            try:
                write_errors(errors_df)
            except Exception as e:
                logger.error("Failed to write errors (continuing anyway): %s", e)
        
        if records_valid == 0:
            logger.info("No valid records after CDE validation - updating checkpoint and exiting")
            update_checkpoint(TOPIC_NAME, max_ts if max_ts else last_checkpoint)
            job.commit()
            return
        
        # ===== STAGE 5: CAST =====
        # Convert types and add audit columns
        current_stage = "CAST"
        typed_df = cast_types(valid_df, schema)
        typed_df = add_audit_columns(typed_df, schema)
        
        # Select only columns defined in schema + audit columns
        schema_columns = list(schema.get('columns', {}).keys())
        audit_columns = list(schema.get('audit_columns', {}).keys())
        all_columns = schema_columns + audit_columns
        available_columns = [c for c in all_columns if c in typed_df.columns]
        final_df = typed_df.select(*available_columns)
        
        # ===== STAGE 6: WRITE =====
        # Save to Curated table
        current_stage = "WRITE"
        is_first_run = True
        try:
            snapshots_df = spark.sql(f"SELECT * FROM {CURATED_TABLE}.snapshots LIMIT 1")
            if snapshots_df.count() > 0:
                is_first_run = False
        except Exception:
            pass
        
        write_to_curated(final_df, is_first_run)
        records_written = final_df.count()
        logger.info("Write complete: %d records", records_written)
        
        # ===== STAGE 7: UPDATE_CHECKPOINT =====
        # Save our progress
        current_stage = "UPDATE_CHECKPOINT"
        update_checkpoint(TOPIC_NAME, max_ts if max_ts else last_checkpoint)
        
        # Print summary
        final_count = spark.table(CURATED_TABLE).count()
        curated_errors = spark.table(ERRORS_TABLE).count()
        
        logger.info("=== SUMMARY ===")
        logger.info("IN: %d | VALID: %d | ERRORS: %d | WRITTEN: %d", 
                    records_in, records_valid, records_error, records_written)
        logger.info("Curated total: %d | Errors total: %d", final_count, curated_errors)
        
        job.commit()
        logger.info("Job completed successfully!")
        
    except Exception as e:
        logger.error("=== JOB FAILED at stage: %s ===", current_stage)
        logger.error("Error: %s", str(e))
        raise


if __name__ == "__main__":
    main()
