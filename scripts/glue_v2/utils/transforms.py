"""
Data Transformation Utilities
=============================
Generic Spark DataFrame manipulations that work across both streaming and
file-based ingestion pipelines.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional, List

# We assume get_logger exists in the new config or utils init
import logging
logger = logging.getLogger(__name__)


def dedup_keep_latest(
    df: DataFrame,
    primary_key: str,
    sort_key: str,
    fallback_sort_key: Optional[str] = None
) -> DataFrame:
    """
    Business Deduplication (LIFO):
    Groups by a primary key and keeps only the latest record based on a sort timestamp.
    
    This replaces the hardcoded `dedup_two_stage` from v1, allowing file-based 
    pipelines (which may not have a network message_id) to use the same logic.
    
    If your pipeline HAS network duplicates, you should run a simple FIFO 
    dedup on message_id first, then call this function for the business dedup.
    
    Args:
        df: Input DataFrame
        primary_key: The business key to deduplicate on (e.g. 'idempotency_key' or 'transaction_id')
        sort_key: The column containing the timestamp to order by (e.g. 'last_updated_ts')
        fallback_sort_key: Optional backup timestamp if sort_key is null (e.g. 'publish_time')
    """
    count_before = df.count()
    if count_before == 0:
        return df

    logger.info(f"Starting deduplication on PK '{primary_key}', sorting by '{sort_key}' (fallback: {fallback_sort_key})")
    
    final_cols = df.columns
    
    # Resolve the sorting timestamp
    if fallback_sort_key:
        df_sortable = df.withColumn(
            "_sort_ts",
            F.coalesce(F.col(sort_key), F.col(fallback_sort_key))
        )
    else:
        df_sortable = df.withColumn("_sort_ts", F.col(sort_key))
        
    # Group by PK, order by timestamp descending (newest first)
    window_lifo = Window.partitionBy(primary_key).orderBy(F.col("_sort_ts").desc())
    
    df_deduped = df_sortable.withColumn("rn", F.row_number().over(window_lifo)) \
                            .filter(F.col("rn") == 1) \
                            .select(*final_cols) # Drop temp columns 'rn' and '_sort_ts'
                            
    count_after = df_deduped.count()
    logger.info(f"Dedup complete: {count_before} -> {count_after} records ({count_before - count_after} duplicates removed)")
    
    return df_deduped


def add_audit_columns(df: DataFrame, schema_version: str = "1.0.0") -> DataFrame:
    """
    Appends standard data governance tracking columns to every record.
    Used before writing to the Curated layer or when landing file data.
    """
    now = F.current_timestamp()
    
    return df.withColumn("first_seen_ts", now) \
             .withColumn("last_updated_ts", now) \
             .withColumn("_schema_version", F.lit(schema_version))


def cast_types(df: DataFrame, schema: dict) -> DataFrame:
    """
    Dynamically converts STRING columns to their proper Spark types 
    based on the JSON schema definition.
    """
    logger.info("Applying schema-driven type casting...")
    
    for col_name, spec in schema.get('columns', {}).items():
        if col_name not in df.columns:
            continue
            
        target_type = str(spec.get('type', 'STRING')).upper()
        
        # Don't cast strings to strings, or complex types not supported natively
        if target_type == 'STRING':
            continue
            
        logger.debug(f"Casting column '{col_name}' to {target_type}")
        
        if target_type.startswith('DECIMAL'):
            spark_type = target_type
        elif target_type in ('INT', 'INTEGER'):
            spark_type = 'int'
        elif target_type == 'BIGINT':
            spark_type = 'bigint'
        elif target_type == 'DOUBLE':
            spark_type = 'double'
        elif target_type == 'TIMESTAMP':
            spark_type = 'timestamp'
        elif target_type == 'BOOLEAN':
            spark_type = 'boolean'
        else:
            logger.warning(f"Unknown target type '{target_type}' for column '{col_name}'. Leaving as string.")
            continue
            
        df = df.withColumn(col_name, F.col(col_name).cast(spark_type))
        
    return df
