"""
Lean-Ops: Unified Dual-Output Job

Single Glue Spark job that reads from RAW once and writes to both:
1. CURATED: Flattened JSON, ALL STRING types, auto-evolving schema
2. SEMANTIC: Schema-governed, TYPED columns, CDE validation

Key Features:
- Independent checkpoints (curated vs semantic)
- Read from min(curated_chkpt, semantic_chkpt) for replay
- Skip layer if already up-to-date
- Tiered failure handling (Curated can succeed independently)

Usage:
    glue-spark-submit unified_job.py \
        --topic_name events \
        --raw_database iceberg_raw_db \
        --curated_database curated_db \
        --semantic_database semantic_db \
        --schema_bucket lean-ops-schemas \
        --iceberg_warehouse s3://bucket/warehouse

Reference: Lean-Ops Implementation Plan
"""

import sys
import json
from datetime import datetime, timezone
from functools import reduce

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

import boto3

# =============================================================================
# INITIALIZATION
# =============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'topic_name',
    'raw_database',
    'curated_database',
    'semantic_database',
    'schema_bucket',
    'iceberg_warehouse',
    'checkpoints_table'
])

TOPIC_NAME = args['topic_name']
RAW_DATABASE = args['raw_database']
CURATED_DATABASE = args['curated_database']
SEMANTIC_DATABASE = args['semantic_database']
SCHEMA_BUCKET = args['schema_bucket']
ICEBERG_WAREHOUSE = args['iceberg_warehouse']
CHECKPOINTS_TABLE = args['checkpoints_table']

# Spark session with Iceberg
spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE) \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .getOrCreate()

glue_context = GlueContext(spark.sparkContext)
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
checkpoints_table = dynamodb.Table(CHECKPOINTS_TABLE)


def log(level, message):
    print(f"[{TOPIC_NAME}] [{level}] {message}")


# =============================================================================
# CHECKPOINT MANAGEMENT
# =============================================================================

def get_checkpoint(layer: str) -> str:
    """Get last processed snapshot for a layer (curated or semantic)."""
    try:
        response = checkpoints_table.get_item(
            Key={'topic_name': TOPIC_NAME, 'layer': layer}
        )
        return response.get('Item', {}).get('last_snapshot', '0')
    except Exception as e:
        log("WARNING", f"Failed to get {layer} checkpoint: {e}")
        return '0'


def update_checkpoint(layer: str, snapshot_id: str):
    """Update checkpoint for a layer."""
    checkpoints_table.put_item(Item={
        'topic_name': TOPIC_NAME,
        'layer': layer,
        'last_snapshot': snapshot_id,
        'updated_at': datetime.now(timezone.utc).isoformat()
    })
    log("INFO", f"Updated {layer} checkpoint to {snapshot_id}")


def get_current_snapshot_id() -> str:
    """Get current RAW table snapshot ID."""
    raw_table = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
    snapshots = spark.sql(f"""
        SELECT snapshot_id 
        FROM {raw_table}.snapshots 
        ORDER BY committed_at DESC 
        LIMIT 1
    """)
    
    if snapshots.count() == 0:
        return None
    return str(snapshots.first()['snapshot_id'])


# =============================================================================
# SCHEMA REGISTRY
# =============================================================================

def load_schema():
    """Load schema from S3 registry."""
    try:
        schema_key = f"{TOPIC_NAME}_semantic_schema.json"
        response = s3.get_object(Bucket=SCHEMA_BUCKET, Key=schema_key)
        schema = json.loads(response['Body'].read().decode('utf-8'))
        log("INFO", f"Loaded schema version {schema.get('version', 'unknown')}")
        return schema
    except Exception as e:
        log("ERROR", f"Failed to load schema: {e}")
        raise


# =============================================================================
# INCREMENTAL READ
# =============================================================================

def read_incremental_raw(snapshot_after: str):
    """Read RAW records since given snapshot."""
    raw_table = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
    
    if snapshot_after == '0':
        log("INFO", "Day 1 mode: reading all records")
        return spark.read.format("iceberg").load(raw_table)
    else:
        log("INFO", f"Incremental mode: reading since snapshot {snapshot_after}")
        return spark.read.format("iceberg") \
            .option("start-snapshot-id", snapshot_after) \
            .load(raw_table)


# =============================================================================
# DEDUPLICATION
# =============================================================================

def apply_dedup(df):
    """Two-stage deduplication: FIFO by message_id, LIFO by idempotency_key."""
    
    # Stage 1: Network dedup - FIFO by message_id (first wins)
    window1 = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    stage1 = df.withColumn("rn", F.row_number().over(window1)) \
               .filter(F.col("rn") == 1) \
               .drop("rn")
    
    count1 = df.count()
    count2 = stage1.count()
    log("INFO", f"Network dedup: {count1} → {count2} ({count1 - count2} removed)")
    
    # Stage 2: App correction dedup - LIFO by idempotency_key (last wins)
    window2 = Window.partitionBy("idempotency_key").orderBy(F.col("ingestion_ts").desc())
    stage2 = stage1.withColumn("rn", F.row_number().over(window2)) \
                   .filter(F.col("rn") == 1) \
                   .drop("rn")
    
    count3 = stage2.count()
    log("INFO", f"App correction dedup: {count2} → {count3} ({count2 - count3} corrections)")
    
    return stage2


# =============================================================================
# CURATED OUTPUT: Flatten to STRING
# =============================================================================

def flatten_json_to_strings(df, schema):
    """Flatten JSON payload to STRING columns based on schema paths."""
    result = df
    
    # Extract all paths defined in schema
    for column_name, json_path in schema.get('json_paths', {}).items():
        result = result.withColumn(
            column_name,
            F.get_json_object(F.col("json_payload"), json_path).cast(StringType())
        )
    
    # Add passthrough columns
    passthrough = schema.get('passthrough_columns', [])
    
    # Add audit columns
    result = result.withColumn("curated_ts", F.current_timestamp().cast(StringType()))
    
    # Select columns for curated table
    columns = list(schema.get('json_paths', {}).keys()) + passthrough + ['curated_ts', 'json_payload']
    existing = [c for c in columns if c in result.columns]
    
    return result.select(*existing)


def detect_new_columns(df, curated_table):
    """Detect columns in df that don't exist in curated table."""
    try:
        existing_df = spark.read.format("iceberg").load(curated_table).limit(0)
        existing_columns = set(existing_df.columns)
        new_columns = set(df.columns) - existing_columns
        return list(new_columns)
    except Exception:
        # Table doesn't exist yet
        return []


def apply_curated_ddl(curated_table, new_columns):
    """Add new STRING columns to curated table."""
    for col in new_columns:
        try:
            spark.sql(f"ALTER TABLE {curated_table} ADD COLUMN {col} STRING")
            log("INFO", f"Added column {col} to curated table")
        except Exception as e:
            if "already exists" not in str(e).lower():
                log("WARNING", f"Failed to add column {col}: {e}")


def write_curated(df, current_snapshot):
    """Write to curated table with auto-evolving schema."""
    curated_table = f"glue_catalog.{CURATED_DATABASE}.{TOPIC_NAME}"
    
    # Detect and add new columns
    new_cols = detect_new_columns(df, curated_table)
    if new_cols:
        log("INFO", f"Detected new columns: {new_cols}")
        apply_curated_ddl(curated_table, new_cols)
    
    # Write to curated
    df.writeTo(curated_table).append()
    
    # Update checkpoint
    update_checkpoint("curated", current_snapshot)
    
    log("INFO", f"Wrote {df.count()} records to curated table")
    return True


# =============================================================================
# SEMANTIC OUTPUT: Schema-governed, Typed
# =============================================================================

def parse_with_schema(df, schema):
    """Parse JSON payload into typed columns based on schema."""
    result = df
    
    for column_name, config in schema.get('columns', {}).items():
        json_path = schema.get('json_paths', {}).get(column_name)
        col_type = config.get('type', 'STRING')
        
        if json_path:
            # Extract from JSON
            result = result.withColumn(
                column_name,
                F.get_json_object(F.col("json_payload"), json_path)
            )
            
            # Cast to correct type
            if col_type == 'INT' or col_type == 'BIGINT':
                result = result.withColumn(column_name, F.col(column_name).cast("bigint"))
            elif col_type == 'DECIMAL':
                result = result.withColumn(column_name, F.col(column_name).cast("decimal(18,2)"))
            elif col_type == 'TIMESTAMP':
                result = result.withColumn(column_name, F.to_timestamp(F.col(column_name)))
            elif col_type == 'BOOLEAN':
                result = result.withColumn(column_name, F.col(column_name).cast("boolean"))
    
    return result


def apply_pii_masking(df, schema):
    """Mask sensitive fields with SHA256."""
    for col_name, config in schema.get('columns', {}).items():
        if config.get('sensitive') and col_name in df.columns:
            df = df.withColumn(col_name, F.sha2(F.col(col_name).cast(StringType()), 256))
            log("INFO", f"Masked sensitive field: {col_name}")
    return df


def validate_cdes(df, schema):
    """Validate CDEs and separate valid from invalid records."""
    cde_columns = [
        col for col, cfg in schema.get('columns', {}).items()
        if cfg.get('cde') and cfg.get('required')
    ]
    
    if not cde_columns:
        return df, spark.createDataFrame([], df.schema)
    
    # Build NULL check
    null_checks = [F.col(c).isNull() for c in cde_columns if c in df.columns]
    if not null_checks:
        return df, spark.createDataFrame([], df.schema)
    
    has_null = reduce(lambda a, b: a | b, null_checks)
    
    errors_df = df.filter(has_null)
    valid_df = df.filter(~has_null)
    
    error_count = errors_df.count()
    if error_count > 0:
        log("WARNING", f"CDE violations: {error_count} records")
    
    return valid_df, errors_df


def write_semantic(df, schema, current_snapshot):
    """Write to semantic table with full schema governance."""
    semantic_table = f"glue_catalog.{SEMANTIC_DATABASE}.{TOPIC_NAME}"
    
    # Parse to typed columns
    parsed = parse_with_schema(df, schema)
    
    # PII masking
    masked = apply_pii_masking(parsed, schema)
    
    # CDE validation
    valid_df, errors_df = validate_cdes(masked, schema)
    
    # Write errors to errors table
    if errors_df.count() > 0:
        errors_table = f"glue_catalog.{SEMANTIC_DATABASE}.errors"
        errors_df.withColumn("topic_name", F.lit(TOPIC_NAME)) \
                 .withColumn("error_type", F.lit("CDE_VIOLATION")) \
                 .withColumn("error_ts", F.current_timestamp()) \
                 .writeTo(errors_table).append()
    
    # Add audit columns
    valid_df = valid_df.withColumn("last_updated_ts", F.current_timestamp())
    
    # Select only schema columns + passthrough + audit
    columns = list(schema.get('columns', {}).keys())
    columns.extend(schema.get('passthrough_columns', []))
    columns.append('last_updated_ts')
    existing = [c for c in columns if c in valid_df.columns]
    final_df = valid_df.select(*existing)
    
    # Write to semantic
    final_df.writeTo(semantic_table).append()
    
    # Update checkpoint
    update_checkpoint("semantic", current_snapshot)
    
    log("INFO", f"Wrote {final_df.count()} records to semantic table")
    return True


# =============================================================================
# MAIN
# =============================================================================

def main():
    log("INFO", f"Starting unified job for {TOPIC_NAME}")
    
    # 1. Get both checkpoints
    curated_chkpt = get_checkpoint("curated")
    semantic_chkpt = get_checkpoint("semantic")
    log("INFO", f"Checkpoints - Curated: {curated_chkpt}, Semantic: {semantic_chkpt}")
    
    # 2. Get current RAW snapshot
    current_snapshot = get_current_snapshot_id()
    if not current_snapshot:
        log("INFO", "No snapshots found - exiting")
        return {"status": "NO_DATA"}
    
    log("INFO", f"Current RAW snapshot: {current_snapshot}")
    
    # 3. Determine what to read (minimum of both checkpoints)
    read_from = min(curated_chkpt, semantic_chkpt, key=lambda x: int(x) if x != '0' else 0)
    
    # 4. Check if any work needed
    curated_needs_update = current_snapshot != curated_chkpt
    semantic_needs_update = current_snapshot != semantic_chkpt
    
    if not curated_needs_update and not semantic_needs_update:
        log("INFO", "Both layers up-to-date - nothing to do")
        return {"status": "UP_TO_DATE"}
    
    # 5. Load schema
    schema = load_schema()
    
    # 6. Read incremental from RAW
    raw_df = read_incremental_raw(read_from)
    
    if raw_df.count() == 0:
        log("INFO", "No new records to process")
        return {"status": "NO_NEW_DATA"}
    
    # 7. Dedup (shared for both outputs)
    deduped = apply_dedup(raw_df)
    deduped.cache()  # Cache since we'll use it twice
    
    # 8. CURATED OUTPUT
    curated_success = False
    if curated_needs_update:
        try:
            curated_df = flatten_json_to_strings(deduped, schema)
            curated_success = write_curated(curated_df, current_snapshot)
        except Exception as e:
            log("ERROR", f"Curated write failed: {e}")
            raise  # Curated failure is critical
    else:
        log("INFO", "Curated already up-to-date - skipping")
        curated_success = True
    
    # 9. SEMANTIC OUTPUT (independent try/catch)
    semantic_success = False
    if semantic_needs_update:
        try:
            semantic_success = write_semantic(deduped, schema, current_snapshot)
        except Exception as e:
            log("ERROR", f"Semantic write failed: {e}")
            # Don't raise - curated can succeed independently
            semantic_success = False
    else:
        log("INFO", "Semantic already up-to-date - skipping")
        semantic_success = True
    
    # 10. Cleanup
    deduped.unpersist()
    
    # 11. Determine outcome
    if curated_success and semantic_success:
        status = "FULL_SUCCESS"
    elif curated_success:
        status = "CURATED_ONLY"
    else:
        status = "FAILED"
    
    log("INFO", f"Job completed with status: {status}")
    return {"status": status, "curated": curated_success, "semantic": semantic_success}


if __name__ == "__main__":
    result = main()
    print(f"JOB_RESULT: {json.dumps(result)}")
    job.commit()
