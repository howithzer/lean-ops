"""
==============================================================================
STREAMING PROCESSOR (Glue V2)
==============================================================================
Reads incoming GCP Pub/Sub transactions from the Iceberg RAW table incrementally 
using snapshots. Applies flattening, deduplication, CDE validation, type-casting, 
schema evolution, and writes directly into the Curated layer.

This script demonstrates replacing the multi-hop (Raw -> Standardized -> Curated)
pipeline with a single unified job using the `glue_v2` utility library.
"""

import sys
import json
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

from utils.config import Config, get_logger
from utils.iceberg_ops import apply_iceberg_catalog_settings, create_table_from_schema
from utils.transforms import dedup_keep_latest, add_audit_columns, cast_types
from utils.validation import validate_cdes, route_errors_to_table
from utils.flatten import flatten_json_payload

logger = get_logger(__name__)


# ----------------------------------------------------------------------------
# DYNAMODB CHECKPOINT HELPERS (Extracted from old scripts)
# ----------------------------------------------------------------------------

def get_dynamo_checkpoint(table_name: str, pk: str) -> str:
    dynamo = boto3.client('dynamodb', region_name="us-east-1")
    try:
        resp = dynamo.get_item(
            TableName=table_name,
            Key={'topic_name': {'S': pk}}
        )
        return resp.get('Item', {}).get('last_snapshot_id', {}).get('S', '0')
    except Exception:
        return '0'


def update_dynamo_checkpoint(table_name: str, pk: str, snapshot_id: str):
    dynamo = boto3.client('dynamodb', region_name="us-east-1")
    dynamo.update_item(
        TableName=table_name,
        Key={'topic_name': {'S': pk}},
        UpdateExpression="SET last_snapshot_id = :val, updated_at = :ts",
        ExpressionAttributeValues={
            ':val': {'S': str(snapshot_id)},
            ':ts': {'S': F.current_timestamp()._jc.toString()} # type: ignore
        }
    )


# ----------------------------------------------------------------------------
# MAIN PROCESSING
# ----------------------------------------------------------------------------

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TOPIC_NAME'])
    topic = args['TOPIC_NAME']
    
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    config = Config.from_env()
    apply_iceberg_catalog_settings(spark, config.iceberg_warehouse)
    
    logger.info(f"=== Starting V2 Stream Processor for Topic: {topic} ===")

    # 1. READ SCHEMA 
    # Let's assume schema is fetched dynamically or from S3 like in file_processor
    # For now, we simulate fetching the schema JSON for this topic
    schema_path = f"s3://{config.iceberg_bucket}/schemas/{topic}.json"
    logger.info(f"Loading schema from {schema_path}")
    try:
        schema_rdd = spark.sparkContext.textFile(schema_path)
        schema = json.loads("".join(schema_rdd.collect()))
    except Exception as e:
        logger.error(f"Failed to load schema from {schema_path}: {e}")
        job.commit()
        return

    # 2. READ INCREMENTAL DATA FROM RAW
    raw_table_path = config.get_table_path(config.raw_database, f"{topic}_staging")
    
    # Get current and last checkpointed snapshots
    current_snapshot = spark.sql(f"SELECT snapshot_id FROM {raw_table_path}.history ORDER BY made_current_at DESC LIMIT 1").collect()
    current_snapshot = str(current_snapshot[0][0]) if current_snapshot else "0"
    
    last_snapshot = get_dynamo_checkpoint(config.checkpoint_table, topic)
    if current_snapshot == last_snapshot or current_snapshot == "0":
        logger.info("No new data in RAW table since last checkpoint. Exiting.")
        job.commit()
        return
        
    logger.info(f"Reading incremental RAW data {last_snapshot} -> {current_snapshot}")
    df_raw = spark.read.format("iceberg") \
                  .option("start-snapshot-id", last_snapshot) \
                  .option("end-snapshot-id", current_snapshot) \
                  .load(raw_table_path)
                  
    if df_raw.count() == 0:
        logger.info("New snapshot contains no readable records. Updating checkpoint and exiting.")
        update_dynamo_checkpoint(config.checkpoint_table, topic, current_snapshot)
        job.commit()
        return
        
    # 3. NETWORK DEDUPLICATION (GCP Pub/Sub retry handling)
    # Stream-specific required step before generalized logic applies
    window_fifo = F.window("message_id") # pseudo-code for original rn=1 filter
    from pyspark.sql.window import Window
    w1 = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    df_net_dedup = df_raw.withColumn("rn", F.row_number().over(w1)).filter(F.col("rn") == 1).drop("rn")

    # 4. FLATTEN
    df_flat = flatten_json_payload(df_net_dedup)

    # 5. BUSINESS DEDUPLICATION
    # Using the generic LIFO deduplication based on schema primary/sort keys
    primary_key = schema.get('primary_key', 'idempotency_key')
    sort_key = schema.get('sort_key', 'last_updated_ts')
    
    # We pass 'publish_time' as fallback because that exists on the envelope
    df_dedup = dedup_keep_latest(df_flat, primary_key, sort_key, fallback_sort_key='publish_time')
    
    # 6. CDE VALIDATION
    df_valid, df_errors = validate_cdes(df_dedup, schema)
    route_errors_to_table(df_errors, config.get_table_path(config.curated_database, f"{topic}_errors"))
    
    if df_valid.count() == 0:
        logger.info("No valid records to write. Updating checkpoint.")
        update_dynamo_checkpoint(config.checkpoint_table, topic, current_snapshot)
        job.commit()
        return

    # 7. TRANSFORM (Type Casting + Audit Tracking)
    df_final = cast_types(df_valid, schema)
    df_final = add_audit_columns(df_final, schema_version=schema.get('version', '1.0'))
    
    # 8. ICEBERG WRITE - Dynamic schema provisioning and MERGE
    curated_table_path = config.get_table_path(config.curated_database, topic)
    curated_location = f"{config.iceberg_warehouse}{config.curated_database}/{topic}"
    
    create_table_from_schema(spark, schema, curated_table_path, curated_location)
    
    # Instead of appending like files, streams use MERGE on the primary key
    merge_condition = f"t.{primary_key} = s.{primary_key}"
    df_final.createOrReplaceTempView("source_batch")
    
    merge_sql = f"""
    MERGE INTO {curated_table_path} t
    USING source_batch s
    ON {merge_condition}
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
    """
    logger.info(f"Executing Stream MERGE into {curated_table_path}")
    spark.sql(merge_sql)
    
    # 9. CHECKPOINT
    update_dynamo_checkpoint(config.checkpoint_table, topic, current_snapshot)
    logger.info("Stream processing complete.")
    job.commit()

if __name__ == "__main__":
    main()
