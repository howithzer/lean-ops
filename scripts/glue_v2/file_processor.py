"""
==============================================================================
FILE-BASED BATCH PROCESSOR (Glue V2)
==============================================================================
Reads CSV/JSON files from an S3 landing zone, applies standard lean-ops 
governance (flattening, CDE validation, type-casting, deduplication, audit columns), 
and writes them directly to Iceberg.

This script demonstrates the power of the `glue_v2` generic utility library.
"""

import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from utils.config import Config, get_logger
from utils.iceberg_ops import apply_iceberg_catalog_settings, create_table_from_schema
from utils.transforms import dedup_keep_latest, add_audit_columns, cast_types
from utils.validation import validate_cdes, route_errors_to_table

logger = get_logger(__name__)

def main():
    # 1. INIT
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 
        'source_path',         # e.g., s3://landing-zone/transactions/2026-03-04/
        'schema_path',         # e.g., s3://schema-bucket/transactions.json
        'target_database',     # e.g., iceberg_curated_db
        'target_table',        # e.g., transactions
        'file_format'          # 'csv' or 'json'
    ])
    
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    config = Config.from_env()
    
    # Configure Iceberg
    apply_iceberg_catalog_settings(spark, config.iceberg_warehouse)
    
    # 2. LOAD SCHEMA
    logger.info(f"Loading schema from {args['schema_path']}")
    schema_rdd = spark.sparkContext.textFile(args['schema_path'])
    schema = json.loads("".join(schema_rdd.collect()))
    
    # 3. READ DATA
    logger.info(f"Reading {args['file_format']} files from {args['source_path']}")
    if args['file_format'].lower() == 'csv':
        df = spark.read.option("header", "true").option("inferSchema", "false").csv(args['source_path'])
    else:
        df = spark.read.json(args['source_path'])
        
    records_in = df.count()
    if records_in == 0:
        logger.info("No records found in source path. Exiting.")
        job.commit()
        return
        
    logger.info(f"Loaded {records_in} records.")
    
    # 4. VALIDATE CDEs
    # Ensures records have critical columns before we try to process them further
    df_valid, df_errors = validate_cdes(df, schema)
    
    error_count = df_errors.count()
    if error_count > 0:
        errors_table_path = config.get_table_path(config.standardized_database, "parse_errors")
        route_errors_to_table(df_errors, errors_table_path)
        
    if df_valid.count() == 0:
        logger.info("No valid records remaining after CDE check. Exiting.")
        job.commit()
        return

    # 5. TRANSFORM (Type Casting & Audit Columns)
    df_transformed = cast_types(df_valid, schema)
    df_transformed = add_audit_columns(df_transformed, schema_version=schema.get('version', '1.0'))
    
    # 6. DEDUPLICATE (Optional step, depends on file nature)
    # If the file contains multiple updates for the same transaction, keep the latest
    primary_key = schema.get('primary_key')
    sort_key = schema.get('sort_key')
    
    if primary_key and sort_key:
        df_final = dedup_keep_latest(df_transformed, primary_key=primary_key, sort_key=sort_key)
    else:
        logger.info("No primary_key/sort_key defined in schema; skipping deduplication.")
        df_final = df_transformed
        
    # 7. WRITE TO ICEBERG
    target_table_path = config.get_table_path(args['target_database'], args['target_table'])
    target_s3_location = f"{config.iceberg_warehouse}{args['target_database']}/{args['target_table']}"
    
    # Dynamically create the partitioned table if it's the very first time we see this schema
    create_table_from_schema(spark, schema, target_table_path, target_s3_location)
    
    logger.info(f"Writing {df_final.count()} records to {target_table_path}")
    df_final.writeTo(target_table_path).append()
    
    logger.info("Batch processing complete.")
    job.commit()

if __name__ == "__main__":
    main()
