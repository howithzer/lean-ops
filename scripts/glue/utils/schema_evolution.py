"""
Schema Evolution Utilities for Curated Processor
=================================================
Provides schema alignment, column addition, and type casting for Iceberg tables.
"""

from typing import List, Set, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .config import (
    get_logger,
    ENVELOPE_COLUMN_VARIANTS,
    PRESERVE_TYPE_COLUMNS,
    MAX_NEW_COLUMNS_PER_RUN,
)

logger = get_logger(__name__)


def add_missing_columns_to_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str
) -> List[str]:
    """
    Add new columns from DataFrame to Curated Iceberg table via ALTER TABLE.
    
    All new columns are added as STRING for maximum flexibility.
    Envelope column variants are excluded to prevent conflicts.
    
    Args:
        spark: Active SparkSession
        df: Source DataFrame with new columns
        table_name: Fully qualified Iceberg table name (e.g., glue_catalog.db.table)
    
    Returns:
        List of newly added column names
    
    Raises:
        Exception: If ALTER TABLE fails for a non-duplicate column
    """
    # Get current curated table columns
    try:
        curated_cols = set(col.lower() for col in spark.table(table_name).columns)
    except Exception as e:
        logger.error("Could not get table columns for %s: %s", table_name, e)
        return []
    
    # Get DataFrame columns (lowercase for comparison)
    df_cols = set(col.lower() for col in df.columns)
    
    # Find new columns not in table AND not envelope variants
    new_cols = df_cols - curated_cols - ENVELOPE_COLUMN_VARIANTS
    
    if not new_cols:
        logger.info("No new columns to add to table")
        return []
    
    logger.info("Schema evolution: %d new columns detected: %s", len(new_cols), sorted(new_cols))
    
    # Apply safety limit
    if len(new_cols) > MAX_NEW_COLUMNS_PER_RUN:
        logger.warning(
            "Too many new columns (%d), limiting to %d",
            len(new_cols),
            MAX_NEW_COLUMNS_PER_RUN
        )
        new_cols = set(sorted(new_cols)[:MAX_NEW_COLUMNS_PER_RUN])
    
    # Add missing columns as STRING
    added_cols: List[str] = []
    for col in sorted(new_cols):
        logger.info("Adding column '%s' as STRING", col)
        try:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col} STRING)")
            added_cols.append(col)
        except Exception as e:
            # Column may already exist (race condition) - that's OK
            if "already exists" in str(e).lower():
                logger.debug("Column '%s' already exists, skipping", col)
            else:
                logger.error("Error adding column '%s': %s", col, e)
    
    if added_cols:
        logger.info("Schema evolution complete: added %d columns", len(added_cols))
    
    return added_cols


def align_dataframe_to_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str
) -> DataFrame:
    """
    Align DataFrame columns to match Curated table schema exactly.
    
    - Columns in table but missing in DF: Added as NULL
    - Columns in DF but not in table: Should be handled by add_missing_columns_to_table first
    - Column order: Reordered to match table schema
    
    Args:
        spark: Active SparkSession
        df: Source DataFrame to align
        table_name: Fully qualified Iceberg table name
    
    Returns:
        DataFrame with columns matching table schema exactly
    """
    try:
        table_cols = spark.table(table_name).columns
    except Exception as e:
        logger.warning("Could not get table schema for alignment: %s", e)
        return df
    
    logger.info("Aligning DataFrame to table schema (%d columns)", len(table_cols))
    
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
                logger.debug("Column '%s' missing in data, inserting NULL", col)
                df = df.withColumn(col, F.lit(None).cast(StringType()))
    
    # Reorder columns to match table schema exactly
    df_aligned = df.select(table_cols)
    
    logger.info("DataFrame aligned: %d columns", len(df_aligned.columns))
    return df_aligned


def safe_cast_to_string(
    df: DataFrame,
    preserve_columns: Optional[Set[str]] = None
) -> DataFrame:
    """
    Safely cast all columns to STRING except those in preserve list.
    
    Handles: INT, LONG, DOUBLE, TIMESTAMP, BOOLEAN, etc.
    This ensures type safety for the "ALL STRING" payload strategy.
    
    Args:
        df: Source DataFrame
        preserve_columns: Column names to preserve original type (default: ingestion_ts)
    
    Returns:
        DataFrame with all non-preserved columns cast to STRING
    """
    if preserve_columns is None:
        preserve_columns = PRESERVE_TYPE_COLUMNS
    
    logger.info("Casting columns to STRING (preserving: %s)", preserve_columns)
    
    cast_count = 0
    for field in df.schema.fields:
        if field.name in preserve_columns:
            continue
        
        if field.dataType != StringType():
            df = df.withColumn(field.name, F.col(field.name).cast(StringType()))
            cast_count += 1
    
    logger.info("Cast %d columns to STRING", cast_count)
    return df
