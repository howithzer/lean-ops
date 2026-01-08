"""
Lean-Ops: Unified Semantic Job

Single Glue Spark job that handles the entire RAW → Semantic transformation:
1. No-data check (short-circuit if no new snapshots)
2. Read incremental from RAW (snapshot-based)
3. Two-stage deduplication (FIFO by message_id, LIFO by idempotency_key)
4. Parse JSON → typed columns (schema registry)
5. PII Masking (sensitive fields)
6. CDE validation → errors table
7. Schema drift detection → drift_log
8. MERGE to Semantic

Usage:
    glue-spark-submit semantic_job.py \
        --topic_name events \
        --raw_database iceberg_raw_db \
        --semantic_database semantic_db \
        --schema_bucket lean-ops-schemas \
        --iceberg_warehouse s3://bucket/warehouse \
        --snapshot_after 0

Reference: Lessons learned from iceberg-v2-firehose-poc
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
from pyspark.sql.types import StringType, LongType, TimestampType

import boto3

# =============================================================================
# INITIALIZATION
# =============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'topic_name',
    'raw_database',
    'semantic_database',
    'schema_bucket',
    'schema_key',
    'iceberg_warehouse',
    'snapshot_after',
    'checkpoints_table'
])

TOPIC_NAME = args['topic_name']
RAW_DATABASE = args['raw_database']
SEMANTIC_DATABASE = args['semantic_database']
SCHEMA_BUCKET = args['schema_bucket']
SCHEMA_KEY = args.get('schema_key', f'{TOPIC_NAME}_semantic_schema.json')
ICEBERG_WAREHOUSE = args['iceberg_warehouse']
SNAPSHOT_AFTER = args.get('snapshot_after', '0')
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

# Logging helper
def log(level, message):
    print(f"[{TOPIC_NAME}] [{level}] {message}")


# =============================================================================
# SCHEMA REGISTRY
# =============================================================================

def load_schema():
    """Load schema from S3 registry."""
    try:
        response = s3.get_object(Bucket=SCHEMA_BUCKET, Key=SCHEMA_KEY)
        schema = json.loads(response['Body'].read().decode('utf-8'))
        log("INFO", f"Loaded schema version {schema.get('version', 'unknown')}")
        return schema
    except Exception as e:
        log("ERROR", f"Failed to load schema: {e}")
        raise


# =============================================================================
# INCREMENTAL READ
# =============================================================================

def get_current_snapshot_id():
    """Get current RAW table snapshot ID."""
    raw_table = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
    snapshots = spark.sql(f"SELECT snapshot_id FROM {raw_table}.snapshots ORDER BY committed_at DESC LIMIT 1")
    
    if snapshots.count() == 0:
        return None
    return str(snapshots.first()['snapshot_id'])


def read_incremental_raw():
    """Read RAW records since last checkpoint."""
    raw_table = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
    
    if SNAPSHOT_AFTER == '0':
        # Day 1: read all
        log("INFO", "Day 1 mode: reading all records")
        return spark.read.format("iceberg").load(raw_table)
    else:
        # Incremental: read since checkpoint
        log("INFO", f"Incremental mode: reading since snapshot {SNAPSHOT_AFTER}")
        return spark.read.format("iceberg") \
            .option("start-snapshot-id", SNAPSHOT_AFTER) \
            .load(raw_table)


# =============================================================================
# DEDUPLICATION
# =============================================================================

def apply_network_dedup(df):
    """Stage 1: Network dedup - FIFO by message_id (first wins)."""
    window = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    
    deduped = df.withColumn("rn", F.row_number().over(window)) \
                .filter(F.col("rn") == 1) \
                .drop("rn")
    
    before = df.count()
    after = deduped.count()
    log("INFO", f"Network dedup: {before} → {after} ({before - after} removed)")
    
    return deduped


def apply_app_correction_dedup(df):
    """Stage 2: App correction dedup - LIFO by idempotency_key (last wins)."""
    window = Window.partitionBy("idempotency_key").orderBy(F.col("ingestion_ts").desc())
    
    deduped = df.withColumn("rn", F.row_number().over(window)) \
                .filter(F.col("rn") == 1) \
                .drop("rn")
    
    before = df.count()
    after = deduped.count()
    log("INFO", f"App correction dedup: {before} → {after} ({before - after} corrections applied)")
    
    return deduped


# =============================================================================
# JSON PARSING
# =============================================================================

def parse_json_with_schema(df, schema):
    """Parse json_payload into typed columns based on schema."""
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
            if col_type == 'INT':
                result = result.withColumn(column_name, F.col(column_name).cast(LongType()))
            elif col_type == 'TIMESTAMP':
                result = result.withColumn(column_name, F.to_timestamp(F.col(column_name)))
    
    # Add passthrough columns
    for col in schema.get('passthrough_columns', []):
        if col not in result.columns:
            log("WARNING", f"Passthrough column {col} not found in source")
    
    return result


# =============================================================================
# PII MASKING
# =============================================================================

def apply_pii_masking(df, schema):
    """Mask sensitive fields with SHA256."""
    sensitive_fields = [
        col for col, cfg in schema.get('columns', {}).items()
        if cfg.get('sensitive')
    ]
    
    for field in sensitive_fields:
        if field in df.columns:
            df = df.withColumn(field, F.sha2(F.col(field).cast(StringType()), 256))
            log("INFO", f"Masked sensitive field: {field}")
    
    return df


# =============================================================================
# CDE VALIDATION
# =============================================================================

def validate_cdes(df, schema):
    """Validate CDEs and route violations to errors table."""
    cde_columns = [
        col for col, cfg in schema.get('columns', {}).items()
        if cfg.get('cde') and cfg.get('required')
    ]
    
    if not cde_columns:
        log("INFO", "No CDE columns to validate")
        return df, spark.createDataFrame([], df.schema)
    
    # Build NULL check filter
    null_checks = [F.col(c).isNull() for c in cde_columns]
    has_null = reduce(lambda a, b: a | b, null_checks)
    
    errors_df = df.filter(has_null) \
                  .withColumn("error_type", F.lit("CDE_VIOLATION")) \
                  .withColumn("error_message", F.lit(f"NULL in required CDE columns: {cde_columns}")) \
                  .withColumn("error_ts", F.current_timestamp())
    
    valid_df = df.filter(~has_null)
    
    error_count = errors_df.count()
    if error_count > 0:
        log("WARNING", f"CDE violations: {error_count} records routed to errors table")
    
    return valid_df, errors_df


def write_errors(errors_df):
    """Write CDE violations to errors table."""
    if errors_df.count() == 0:
        return
    
    errors_table = f"glue_catalog.{SEMANTIC_DATABASE}.errors"
    
    errors_df.select(
        F.lit(TOPIC_NAME).alias("topic_name"),
        "message_id",
        "idempotency_key",
        "error_type",
        "error_message",
        "error_ts",
        "json_payload"
    ).writeTo(errors_table).append()
    
    log("INFO", f"Wrote {errors_df.count()} errors to {errors_table}")


# =============================================================================
# SCHEMA DRIFT DETECTION
# =============================================================================

def detect_and_log_drift(df, schema):
    """Detect schema drift and log to drift_log table."""
    expected_paths = set(schema.get('json_paths', {}).values())
    
    # Sample actual paths from data
    actual_paths = set()
    sample = df.select("json_payload").limit(100).collect()
    
    def get_all_paths(obj, prefix="$"):
        paths = set()
        if isinstance(obj, dict):
            for key, value in obj.items():
                path = f"{prefix}.{key}"
                paths.add(path)
                paths.update(get_all_paths(value, path))
        elif isinstance(obj, list) and obj:
            paths.update(get_all_paths(obj[0], f"{prefix}[*]"))
        return paths
    
    for row in sample:
        try:
            parsed = json.loads(row.json_payload)
            actual_paths.update(get_all_paths(parsed))
        except:
            pass
    
    new_fields = actual_paths - expected_paths
    missing_fields = expected_paths - actual_paths
    
    if new_fields or missing_fields:
        drift_table = f"glue_catalog.{SEMANTIC_DATABASE}.drift_log"
        
        drift_record = spark.createDataFrame([{
            'topic_name': TOPIC_NAME,
            'detected_at': datetime.now(timezone.utc).isoformat(),
            'new_fields': json.dumps(list(new_fields)),
            'missing_fields': json.dumps(list(missing_fields)),
            'schema_version': schema.get('version', 'unknown')
        }])
        
        drift_record.writeTo(drift_table).append()
        
        log("WARNING", f"Schema drift detected: +{len(new_fields)} new, -{len(missing_fields)} missing")
    else:
        log("INFO", "No schema drift detected")


# =============================================================================
# MERGE TO SEMANTIC
# =============================================================================

def merge_to_semantic(df, schema):
    """MERGE (upsert) to semantic table."""
    semantic_table = f"glue_catalog.{SEMANTIC_DATABASE}.{TOPIC_NAME}"
    
    # Select columns based on schema + passthrough + audit
    columns = list(schema.get('columns', {}).keys())
    columns.extend(schema.get('passthrough_columns', []))
    columns.extend(['first_seen_ts', 'last_updated_ts'])
    
    # Add audit columns
    df = df.withColumn("first_seen_ts", F.current_timestamp()) \
           .withColumn("last_updated_ts", F.current_timestamp())
    
    # Select only columns that exist
    existing_cols = [c for c in columns if c in df.columns]
    final_df = df.select(*existing_cols)
    
    if SNAPSHOT_AFTER == '0':
        # Day 1: overwrite
        log("INFO", f"Day 1 mode: overwriting {semantic_table}")
        final_df.writeTo(semantic_table).overwritePartitions()
    else:
        # Incremental: MERGE
        log("INFO", f"Incremental mode: MERGE to {semantic_table}")
        final_df.createOrReplaceTempView("updates")
        
        # Build MERGE statement
        merge_key = "idempotency_key"
        update_cols = [c for c in existing_cols if c not in [merge_key, 'first_seen_ts']]
        
        set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
        insert_cols = ", ".join(existing_cols)
        insert_vals = ", ".join([f"s.{c}" for c in existing_cols])
        
        spark.sql(f"""
            MERGE INTO {semantic_table} t
            USING updates s
            ON t.{merge_key} = s.{merge_key}
            WHEN MATCHED THEN UPDATE SET {set_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
    
    log("INFO", f"Successfully merged {final_df.count()} records")


# =============================================================================
# CHECKPOINT UPDATE
# =============================================================================

def update_checkpoint(snapshot_id):
    """Update checkpoint in DynamoDB."""
    table = dynamodb.Table(CHECKPOINTS_TABLE)
    table.put_item(Item={
        'topic_name': TOPIC_NAME,
        'last_snapshot': snapshot_id,
        'last_run_ts': datetime.now(timezone.utc).isoformat(),
        'status': 'SUCCESS'
    })
    log("INFO", f"Updated checkpoint to snapshot {snapshot_id}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    log("INFO", f"Starting semantic job for {TOPIC_NAME}")
    
    # 1. Load schema
    schema = load_schema()
    
    # 2. Get current snapshot
    current_snapshot = get_current_snapshot_id()
    if not current_snapshot:
        log("INFO", "No snapshots found - exiting")
        return
    
    if current_snapshot == SNAPSHOT_AFTER:
        log("INFO", "No new data since last run - short-circuit exit")
        return
    
    # 3. Read incremental from RAW
    raw_df = read_incremental_raw()
    
    if raw_df.count() == 0:
        log("INFO", "No records to process")
        return
    
    # 4. Two-stage deduplication
    deduped = apply_network_dedup(raw_df)
    deduped = apply_app_correction_dedup(deduped)
    
    # 5. Parse JSON
    parsed = parse_json_with_schema(deduped, schema)
    
    # 6. PII Masking
    masked = apply_pii_masking(parsed, schema)
    
    # 7. CDE validation
    valid, errors = validate_cdes(masked, schema)
    write_errors(errors)
    
    # 8. Schema drift detection
    detect_and_log_drift(parsed, schema)
    
    # 9. MERGE to semantic
    merge_to_semantic(valid, schema)
    
    # 10. Update checkpoint
    update_checkpoint(current_snapshot)
    
    log("INFO", f"Semantic job completed for {TOPIC_NAME}")


if __name__ == "__main__":
    main()
    job.commit()
