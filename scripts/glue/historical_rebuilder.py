"""
==============================================================================
HISTORICAL REBUILDER PROCESSOR
==============================================================================
What does this script do? 
-------------------------
When a braking schema change occurs (NUCLEAR_RESET), the Standardized array 
must be rebuilt from scratch using the new Glue Catalog schema. This job:
1. Reads ALL historical RAW data (ignoring bookmarks/checkpoints)
2. Throws away duplicate network retries (FIFO)
3. Unpacks envelopes against the NEW schema explicitly 
4. Maps internal columns
5. Appends all data into the newly recreated Standardized Iceberg table
6. Sets the DynamoDB bookmark to the newest snapshot processed so the 
   standard pipeline can resume.

Run by: AWS Step Functions (triggered by Schema Deployer)
Input:  iceberg_raw_db.{topic}_staging
Output: iceberg_standardized_db.{topic}
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
    safe_cast_to_string,
    align_dataframe_to_table
)

logger = get_logger(__name__)


# ==============================================================================
# CONFIGURATION 
# ==============================================================================

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',           
    'target_std_table',       # E.g., 'std_trades'
    'source_topics',          # JSON string array: '["topic_trades_v1", "topic_trades_v2"]'
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

TARGET_TABLE_NAME = args['target_std_table']
SOURCE_TOPICS = json.loads(args['source_topics'])
RAW_DATABASE = args['raw_database']
STANDARDIZED_DATABASE = args['standardized_database']
CHECKPOINT_TABLE = args['checkpoint_table']
ICEBERG_BUCKET = args['iceberg_bucket']
ICEBERG_WAREHOUSE = f"s3://{ICEBERG_BUCKET}/"

for key, value in ICEBERG_CATALOG_SETTINGS.items():
    spark.conf.set(key, value)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)

TARGET_TABLE = f"glue_catalog.{STANDARDIZED_DATABASE}.{TARGET_TABLE_NAME}"


def get_current_snapshot_id(table_path: str) -> str:
    try:
        snapshots_df = spark.sql(
            f"SELECT snapshot_id FROM {table_path}.snapshots ORDER BY committed_at DESC LIMIT 1"
        )
        if snapshots_df.count() > 0:
            return str(snapshots_df.first()['snapshot_id'])
    except Exception as e:
        logger.warning("Could not get snapshot ID for %s: %s", table_path, e)
    return "0"


def update_checkpoint(pipeline_id: str, snapshot_id: str, max_ingestion_ts: int) -> None:
    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(CHECKPOINT_TABLE)
    
    table.put_item(Item={
        'pipeline_id': pipeline_id,
        'checkpoint_type': 'standardized',
        'last_snapshot_id': snapshot_id,
        'last_ingestion_ts': max_ingestion_ts,
        'updated_at': datetime.utcnow().isoformat()
    })
    logger.info("Bookmark saved: pipeline=%s, snapshot=%s", pipeline_id, snapshot_id)


def dedup_fifo(df):
    logger.info("Starting historical FIFO deduplication...")
    window_fifo = Window.partitionBy("message_id").orderBy(F.col("ingestion_ts").asc())
    df_fifo = df.withColumn("rn", F.row_number().over(window_fifo)) \
                .filter(F.col("rn") == 1) \
                .drop("rn")
    logger.info("FIFO dedup complete.")
    return df_fifo


def main():
    current_stage = "INIT"
    records_in = 0
    records_valid = 0
    records_error = 0
    records_written = 0
    
    try:
        logger.info("=== Starting Historical Rebuilder ===")
        logger.info("Target: %s", TARGET_TABLE)
        logger.info("Sources: %s", SOURCE_TOPICS)
        
        # ===== STAGE 1: READ ALL RAW HISTORY =====
        current_stage = "READ"
        raw_dfs = []
        topic_snapshots = {}

        for topic in SOURCE_TOPICS:
            raw_table = f"glue_catalog.{RAW_DATABASE}.{topic}_staging"
            logger.info("Reading FULL HISTORY from %s", raw_table)
            
            try:
                # Full table scan (no start/end snapshot provided)
                df = spark.read.format("iceberg").load(raw_table)
                raw_dfs.append(df)
                
                # Capture the current snapshot so we can bookmark it later
                topic_snapshots[topic] = get_current_snapshot_id(raw_table)
            except Exception as e:
                logger.error("Failed to read history for %s: %s", topic, e)
                raise
                
        if not raw_dfs:
            logger.info("No RAW data found across any source topics.")
            job.commit()
            return

        # Union all topics together
        combined_raw_df = raw_dfs[0]
        for df in raw_dfs[1:]:
            combined_raw_df = combined_raw_df.unionByName(df, allowMissingColumns=True)

        records_in = combined_raw_df.count()
        logger.info("Total historical records to process: %d", records_in)
        
        max_ingestion_ts = combined_raw_df.agg(F.max("ingestion_ts")).collect()[0][0]
        max_ingestion_ts = int(max_ingestion_ts) if max_ingestion_ts else 0

        # ===== STAGE 2: VALIDATE =====
        current_stage = "VALIDATE"
        df_deduped = dedup_fifo(combined_raw_df)
        
        df_checked = df_deduped.withColumn("_trimmed", F.trim(F.col("json_payload"))) \
            .withColumn("is_valid_json", F.when(
                (F.col("json_payload").isNotNull()) & (F.col("_trimmed") != "") &
                ((F.col("_trimmed").startswith("{") & F.col("_trimmed").endswith("}")) |
                 (F.col("_trimmed").startswith("[") & F.col("_trimmed").endswith("]"))),
                F.lit(True)
            ).otherwise(F.lit(False))).drop("_trimmed")
        
        df_valid = df_checked.filter(F.col("is_valid_json") == True).drop("is_valid_json")
        df_invalid = df_checked.filter(F.col("is_valid_json") == False)
        
        records_error = df_invalid.count()
        records_valid = df_valid.count()

        if records_error > 0:
            logger.warn("Found %d invalid historical records - sending to parse_errors", records_error)
            error_df = df_invalid.select(
                F.col("json_payload").alias("raw_payload"),
                F.lit("INVALID_JSON_HISTORICAL").alias("error_type"),
                F.lit("JSON validation failed during historical rebuild").alias("error_message"),
                F.current_timestamp().alias("processed_ts")
            )
            try:
                error_df.writeTo(f"glue_catalog.{STANDARDIZED_DATABASE}.parse_errors").append()
            except Exception as e:
                logger.error("Failed to write errors: %s", e)

        if records_valid == 0:
            logger.info("No valid records found.")
            for topic, snap_id in topic_snapshots.items():
                pipeline_id = f"standardization_{topic}"
                update_checkpoint(pipeline_id, snap_id, max_ingestion_ts)
            job.commit()
            return

        # ===== STAGE 3: FLATTEN (Against NEW Schema) =====
        current_stage = "FLATTEN"
        df_flattened = flatten_json_payload(df_valid)
        
        column_mappings = {
            "_metadata_idempotencykeyresource": "idempotency_key",
            "_metadata_periodreference": "period_reference",
            "_metadata_correlationid": "correlation_id",
        }
        for source_col, target_col in column_mappings.items():
            if source_col in df_flattened.columns:
                if target_col not in df_flattened.columns:
                    df_flattened = df_flattened.withColumn(target_col, F.col(source_col))
                else:
                    df_flattened = df_flattened.withColumn(target_col, F.coalesce(F.col(target_col), F.col(source_col)))
                df_flattened = df_flattened.drop(source_col)
                
        df_flattened = safe_cast_to_string(df_flattened)
        df_aligned = align_dataframe_to_table(spark, df_flattened, TARGET_TABLE)

        # ===== STAGE 4: APPEND =====
        current_stage = "APPEND"
        # We use APPEND instead of MERGE because the table is brand new (empty)
        # This is significantly faster for historical rebuilds than merges.
        logger.info("Writing completely new history to %s...", TARGET_TABLE)
        
        # Repartition for better write performance on massive historical loads
        df_aligned.repartition(10).writeTo(TARGET_TABLE).append()
        records_written = records_valid
        
        # ===== STAGE 5: SET CHECKPOINTS =====
        current_stage = "CHECKPOINT"
        # Since this table was fed from multiple topics, we update the 
        # checkpoint bookmark for EVERY source topic that contributed.
        for topic, snap_id in topic_snapshots.items():
            pipeline_id = f"standardization_{topic}"
            update_checkpoint(pipeline_id, snap_id, max_ingestion_ts)
            
        logger.info("=== HISTORICAL REBUILD COMPLETE ===")
        logger.info("IN: %d | WRITTEN: %d | ERRORS: %d", records_in, records_written, records_error)
        
        job.commit()
        
    except Exception as e:
        logger.error("=== JOB FAILED at stage: %s ===", current_stage)
        logger.error("Error: %s", str(e))
        raise

if __name__ == "__main__":
    main()
