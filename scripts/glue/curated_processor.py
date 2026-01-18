"""
Curated Layer Processor - PySpark Glue Job
============================================
Standardized → Curated processing with:
- Schema-driven type casting (STRING → INT/TIMESTAMP/DECIMAL)
- CDE (Critical Data Element) validation
- Error routing to errors table
- Incremental processing via timestamp-based checkpoint
- MERGE on idempotency_key

Author: lean-ops team
Version: 1.0.0
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


# =============================================================================
# CONFIGURATION
# =============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'topic_name',
    'standardized_database',
    'curated_database',
    'checkpoint_table',
    'iceberg_bucket',
    'schema_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TOPIC_NAME = args['topic_name']
STANDARDIZED_DATABASE = args['standardized_database']
CURATED_DATABASE = args['curated_database']
CHECKPOINT_TABLE = args['checkpoint_table']
ICEBERG_BUCKET = args['iceberg_bucket']
SCHEMA_BUCKET = args['schema_bucket']
ICEBERG_WAREHOUSE = f"s3://{ICEBERG_BUCKET}/"

# Configure Iceberg catalog
for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

# Full table paths
STANDARDIZED_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.events"
CURATED_TABLE = f"glue_catalog.{CURATED_DATABASE}.events"
ERRORS_TABLE = f"glue_catalog.{CURATED_DATABASE}.errors"


# =============================================================================
# SCHEMA LOADING
# =============================================================================

def load_schema(bucket: str, key: str) -> dict:
    """Load curated schema from S3."""
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


# =============================================================================
# CHECKPOINT FUNCTIONS
# =============================================================================

def get_last_checkpoint(topic_name: str) -> str:
    """Get last processed timestamp from DynamoDB checkpoint table."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    try:
        response = table.get_item(Key={
            'pipeline_id': f'curated_{topic_name}',
            'checkpoint_type': 'curated'
        })
        if 'Item' in response:
            checkpoint = response['Item'].get('last_updated_ts', '1970-01-01T00:00:00')
            logger.info("Retrieved checkpoint for curated_%s: %s", topic_name, checkpoint)
            return checkpoint
    except Exception as e:
        logger.error("Error getting checkpoint: %s", e)
    
    return '1970-01-01T00:00:00'


def update_checkpoint(topic_name: str, checkpoint_value: str) -> None:
    """Update checkpoint in DynamoDB."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    table.put_item(Item={
        'pipeline_id': f'curated_{topic_name}',
        'checkpoint_type': 'curated',
        'last_updated_ts': checkpoint_value,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    logger.info("Checkpoint updated: curated_%s -> %s", topic_name, checkpoint_value)


# =============================================================================
# CDE VALIDATION
# =============================================================================

def validate_cdes(df, schema: dict):
    """
    Validate Critical Data Elements.
    Returns: (valid_df, errors_df)
    """
    columns = schema.get('columns', {})
    cde_columns = [col for col, spec in columns.items() if spec.get('cde', False)]
    
    logger.info("CDE columns to validate: %s", cde_columns)
    
    # Build validation condition: all CDEs must be non-null
    conditions = []
    for col_name in cde_columns:
        spec = columns[col_name]
        if not spec.get('nullable', True):
            conditions.append(F.col(col_name).isNotNull())
    
    if not conditions:
        # No CDE validation needed
        return df, None
    
    # Combine conditions with AND
    valid_condition = conditions[0]
    for cond in conditions[1:]:
        valid_condition = valid_condition & cond
    
    # Split into valid and invalid
    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition)
    
    # Add error metadata to invalid records
    if invalid_df.count() > 0:
        errors_df = invalid_df.select(
            F.col("message_id"),
            F.col("idempotency_key"),
            F.to_json(F.struct(*[F.col(c) for c in df.columns])).alias("raw_record"),
            F.lit("CDE_VIOLATION").alias("error_type"),
            F.lit(",".join(cde_columns)).alias("error_field"),
            F.lit("Required CDE field is null").alias("error_message"),
            F.current_timestamp().alias("processed_ts")
        )
        logger.info("CDE validation: %d valid, %d invalid", valid_df.count(), invalid_df.count())
        return valid_df, errors_df
    
    return valid_df, None


# =============================================================================
# TYPE CASTING
# =============================================================================

def cast_types(df, schema: dict):
    """Cast columns from STRING to typed values based on schema."""
    columns = schema.get('columns', {})
    
    for col_name, spec in columns.items():
        if col_name not in df.columns:
            continue
            
        target_type = spec.get('type', 'STRING')
        
        if target_type == 'INT':
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
        elif target_type == 'TIMESTAMP':
            # Try parsing as ISO timestamp
            df = df.withColumn(col_name, F.to_timestamp(F.col(col_name)))
        elif target_type == 'DOUBLE':
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))
        elif target_type.startswith('DECIMAL'):
            # Extract precision and scale from DECIMAL(10,2)
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
        # STRING remains unchanged
    
    logger.info("Type casting complete")
    return df


# =============================================================================
# ADD AUDIT COLUMNS
# =============================================================================

def add_audit_columns(df, schema: dict):
    """Add audit columns (first_seen_ts, last_updated_ts, _schema_version)."""
    now = F.current_timestamp()
    schema_version = schema.get('audit_columns', {}).get('_schema_version', {}).get('default', '1.0.0')
    
    df = df.withColumn("first_seen_ts", now)
    df = df.withColumn("last_updated_ts", now)
    df = df.withColumn("_schema_version", F.lit(schema_version))
    
    return df


# =============================================================================
# WRITE FUNCTIONS
# =============================================================================

def write_errors(errors_df):
    """Write error records to errors table."""
    if errors_df is None or errors_df.count() == 0:
        logger.info("No errors to write")
        return
    
    error_count = errors_df.count()
    errors_df.writeTo(ERRORS_TABLE).using("iceberg").append()
    logger.info("Wrote %d error records to %s", error_count, ERRORS_TABLE)


def write_to_curated(df, is_first_run: bool):
    """Write valid records to Curated table using MERGE."""
    if df.count() == 0:
        logger.info("No valid records to write")
        return
    
    if is_first_run:
        logger.info("First run - using writeTo() for initial load")
        df.writeTo(CURATED_TABLE).using("iceberg").createOrReplace()
        logger.info("Initial data loaded successfully")
    else:
        # MERGE for subsequent runs
        df.createOrReplaceTempView("staged_data")
        
        columns = df.columns
        update_clause = ", ".join([f"t.{c} = s.{c}" for c in columns if c != 'idempotency_key'])
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join([f"s.{c}" for c in columns])
        
        merge_sql = f"""
        MERGE INTO {CURATED_TABLE} t
        USING staged_data s
        ON t.idempotency_key = s.idempotency_key
        WHEN MATCHED AND s.last_updated_ts > t.last_updated_ts THEN
            UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        logger.info("Executing MERGE...")
        spark.sql(merge_sql)
        logger.info("MERGE complete")


# =============================================================================
# MAIN PROCESSING
# =============================================================================

def main():
    """Main entry point for Curated processing."""
    logger.info("Starting Curated processing for topic: %s", TOPIC_NAME)
    
    # Load schema
    schema = load_schema(SCHEMA_BUCKET, "schemas/curated_schema.json")
    
    # Get last checkpoint (timestamp-based)
    last_checkpoint = get_last_checkpoint(TOPIC_NAME)
    logger.info("Last checkpoint: %s", last_checkpoint)
    
    # Read Standardized data incrementally (timestamp-based, NOT snapshot)
    standardized_df = spark.table(STANDARDIZED_TABLE).filter(
        F.col("last_updated_ts") > F.lit(last_checkpoint).cast(TimestampType())
    )
    
    record_count = standardized_df.count()
    logger.info("Records to process: %d", record_count)
    
    if record_count == 0:
        logger.info("No new records to process")
        job.commit()
        return
    
    # Get max timestamp for new checkpoint
    max_ts = standardized_df.agg(F.max("last_updated_ts")).collect()[0][0]
    max_ts_str = max_ts.isoformat() if max_ts else last_checkpoint
    logger.info("Max last_updated_ts: %s", max_ts_str)
    
    # Validate CDEs
    valid_df, errors_df = validate_cdes(standardized_df, schema)
    
    # Write errors
    write_errors(errors_df)
    
    # Cast types
    typed_df = cast_types(valid_df, schema)
    
    # Add audit columns
    typed_df = add_audit_columns(typed_df, schema)
    
    # Select only columns defined in schema + audit
    schema_columns = list(schema.get('columns', {}).keys())
    audit_columns = list(schema.get('audit_columns', {}).keys())
    passthrough = schema.get('passthrough_columns', [])
    
    all_columns = schema_columns + audit_columns
    available_columns = [c for c in all_columns if c in typed_df.columns]
    final_df = typed_df.select(*available_columns)
    
    # Check if first run
    is_first_run = True
    try:
        snapshots_df = spark.sql(f"SELECT * FROM {CURATED_TABLE}.snapshots LIMIT 1")
        if snapshots_df.count() > 0:
            is_first_run = False
            logger.info("Curated table has existing snapshots")
    except Exception as e:
        logger.info("First run detected (no snapshots): %s", e)
    
    # Write to Curated
    write_to_curated(final_df, is_first_run)
    
    # Update checkpoint
    update_checkpoint(TOPIC_NAME, max_ts_str)
    
    # Get final count
    final_count = spark.table(CURATED_TABLE).count()
    logger.info("Curated table now has %d total records", final_count)
    
    job.commit()
    logger.info("Job completed successfully")


if __name__ == "__main__":
    main()
