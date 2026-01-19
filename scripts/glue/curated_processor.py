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

def get_last_checkpoint(topic_name: str) -> int:
    """Get last processed ingestion_ts (epoch) from DynamoDB checkpoint table."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    try:
        response = table.get_item(Key={
            'pipeline_id': f'curated_{topic_name}',
            'checkpoint_type': 'curated'
        })
        if 'Item' in response:
            # ingestion_ts is stored as BIGINT (epoch milliseconds)
            checkpoint = int(response['Item'].get('last_ingestion_ts', 0))
            logger.info("Retrieved checkpoint for curated_%s: %d", topic_name, checkpoint)
            return checkpoint
    except Exception as e:
        logger.error("Error getting checkpoint: %s", e)
    
    return 0


def update_checkpoint(topic_name: str, checkpoint_value: int) -> None:
    """Update checkpoint (ingestion_ts epoch) in DynamoDB."""
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    table.put_item(Item={
        'pipeline_id': f'curated_{topic_name}',
        'checkpoint_type': 'curated',
        'last_ingestion_ts': checkpoint_value,
        'updated_at': datetime.utcnow().isoformat()
    })
    
    logger.info("Checkpoint updated: curated_%s -> %d", topic_name, checkpoint_value)


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


def add_missing_columns_to_curated(df):
    """
    Add missing columns from DataFrame to the Curated Iceberg table.
    
    This enables schema evolution for MERGE - if the source DataFrame has
    columns that don't exist in the target table, we add them first.
    
    Args:
        df: Source DataFrame with potentially new columns
    """
    try:
        # Get existing columns from target table
        target_df = spark.table(CURATED_TABLE).limit(0)  # Just schema, no data
        target_cols = set(c.lower() for c in target_df.columns)
        source_cols = set(c.lower() for c in df.columns)
        
        # Find new columns
        new_cols = source_cols - target_cols
        
        if not new_cols:
            logger.info("No schema evolution needed - all columns exist in target")
            return
        
        logger.info("Schema evolution: adding %d new columns: %s", len(new_cols), sorted(new_cols))
        
        # Add each new column (Iceberg supports ALTER TABLE ADD COLUMN)
        for col in sorted(new_cols):
            # All columns from Standardized are STRING
            alter_sql = f"ALTER TABLE {CURATED_TABLE} ADD COLUMN {col} STRING"
            try:
                spark.sql(alter_sql)
                logger.info("Added column: %s", col)
            except Exception as e:
                # Column might already exist (race condition) - that's OK
                if "already exists" in str(e).lower():
                    logger.info("Column %s already exists", col)
                else:
                    logger.warning("Failed to add column %s: %s", col, e)
    
    except Exception as e:
        logger.warning("Schema evolution check failed (table may not exist): %s", e)


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
        # Schema evolution: add any new columns before MERGE
        add_missing_columns_to_curated(df)
        
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
    """
    Main entry point for Curated processing.
    
    Stages:
        1. INIT: Load schema (with validation)
        2. CHECKPOINT: Get last checkpoint
        3. READ: Read Standardized data
        4. CDE_VALIDATE: Validate Critical Data Elements
        5. CAST: Type casting
        6. WRITE: MERGE to Curated table
        7. UPDATE_CHECKPOINT: Update DynamoDB
    """
    current_stage = "INIT"
    records_in = 0
    records_valid = 0
    records_error = 0
    records_written = 0
    
    try:
        logger.info("Starting Curated processing for topic: %s", TOPIC_NAME)
        
        # === STAGE 1: INIT (Load and Validate Schema) ===
        current_stage = "INIT"
        try:
            schema = load_schema(SCHEMA_BUCKET, "schemas/curated_schema.json")
        except Exception as e:
            logger.error("Failed to load curated schema: %s", e)
            raise RuntimeError(f"Schema loading failed: {e}")
        
        # Validate schema structure
        if not schema.get('columns'):
            raise RuntimeError("Schema missing 'columns' definition")
        logger.info("Schema loaded successfully with %d columns", len(schema.get('columns', {})))
        
        # === STAGE 2: CHECKPOINT ===
        current_stage = "CHECKPOINT"
        last_checkpoint = get_last_checkpoint(TOPIC_NAME)
        logger.info("Last checkpoint (ingestion_ts): %d", last_checkpoint)
        
        # === STAGE 3: READ ===
        current_stage = "READ"
        standardized_df = spark.table(STANDARDIZED_TABLE).filter(
            F.col("ingestion_ts") > F.lit(last_checkpoint)
        )
        records_in = standardized_df.count()
        logger.info("Records to process: %d", records_in)
        
        if records_in == 0:
            logger.info("No new records to process - exiting gracefully")
            job.commit()
            return
        
        max_ts = standardized_df.agg(F.max("ingestion_ts")).collect()[0][0]
        logger.info("Max ingestion_ts: %d", max_ts if max_ts else 0)
        
        # === STAGE 4: CDE_VALIDATE ===
        current_stage = "CDE_VALIDATE"
        valid_df, errors_df = validate_cdes(standardized_df, schema)
        records_valid = valid_df.count()
        records_error = errors_df.count() if errors_df else 0
        
        # Write CDE errors (non-fatal if write fails)
        if errors_df and records_error > 0:
            try:
                write_errors(errors_df)
            except Exception as e:
                logger.error("Failed to write CDE errors (non-fatal): %s", e)
        
        if records_valid == 0:
            logger.info("No valid records after CDE validation - updating checkpoint and exiting")
            update_checkpoint(TOPIC_NAME, max_ts if max_ts else last_checkpoint)
            job.commit()
            return
        
        # === STAGE 5: CAST ===
        current_stage = "CAST"
        typed_df = cast_types(valid_df, schema)
        typed_df = add_audit_columns(typed_df, schema)
        
        # Select only columns defined in schema + audit
        schema_columns = list(schema.get('columns', {}).keys())
        audit_columns = list(schema.get('audit_columns', {}).keys())
        all_columns = schema_columns + audit_columns
        available_columns = [c for c in all_columns if c in typed_df.columns]
        final_df = typed_df.select(*available_columns)
        
        # === STAGE 6: WRITE ===
        current_stage = "WRITE"
        is_first_run = True
        try:
            snapshots_df = spark.sql(f"SELECT * FROM {CURATED_TABLE}.snapshots LIMIT 1")
            if snapshots_df.count() > 0:
                is_first_run = False
        except Exception:
            pass  # First run
        
        write_to_curated(final_df, is_first_run)
        records_written = final_df.count()
        logger.info("WRITE complete: %d records", records_written)
        
        # === STAGE 7: UPDATE_CHECKPOINT ===
        current_stage = "UPDATE_CHECKPOINT"
        update_checkpoint(TOPIC_NAME, max_ts if max_ts else last_checkpoint)
        
        # Final summary
        final_count = spark.table(CURATED_TABLE).count()
        curated_errors = spark.table(ERRORS_TABLE).count()
        
        logger.info("=== PROCESSING SUMMARY ===")
        logger.info("Records IN: %d", records_in)
        logger.info("Records VALID: %d", records_valid)
        logger.info("Records CDE_ERROR: %d", records_error)
        logger.info("Records WRITTEN: %d", records_written)
        logger.info("Curated TOTAL: %d", final_count)
        logger.info("Curated ERRORS: %d", curated_errors)
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
