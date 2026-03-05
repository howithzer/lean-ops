"""
Validation Utilities
====================
Critical Data Element (CDE) validation and error handling for datasets.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def validate_cdes(df: DataFrame, schema: dict) -> Tuple[DataFrame, DataFrame]:
    """
    Checks that all required fields marked as "CDE" have non-null values.
    
    Args:
        df: Input DataFrame to check
        schema: The JSON schema specifying which columns are CDEs
        
    Returns:
        (valid_df, errors_df): A tuple of records that passed and records that failed
    """
    logger.info("Validating Critical Data Elements (CDEs)...")
    
    # 1. Read CDE list directly from the JSON schema definition
    cde_columns = []
    for col_name, specs in schema.get('columns', {}).items():
        # A column is a CDE if 'cde': true, or if 'nullable': false and we want strict checks
        if specs.get('cde', False):
            cde_columns.append(col_name)
    
    if not cde_columns:
        logger.info("No CDEs defined in schema. All records valid.")
        return df, df.filter(F.lit(False))
        
    logger.info(f"Checking CDEs: {cde_columns}")
    
    # 2. Build explicit error tracking arrays
    #    Iterating cleanly over each CDE requirement
    df_checked = df
    error_reasons_col = F.array()
    
    for cde in cde_columns:
        if cde in df_checked.columns:
            # Append to array if the column is null or empty string
            is_invalid = F.col(cde).isNull() | (F.col(cde) == "")
            error_reasons_col = F.when(
                is_invalid,
                F.array_append(error_reasons_col, F.lit(f"Missing CDE: {cde}"))
            ).otherwise(error_reasons_col)
        else:
            # If the column literally doesn't exist in the data frame yet
            error_reasons_col = F.array_append(error_reasons_col, F.lit(f"Missing CDE column entirely: {cde}"))
            
    # Mount the errors array
    df_checked = df_checked.withColumn("_cde_errors", error_reasons_col)
    
    # Validation split
    df_valid = df_checked.filter(F.size(F.col("_cde_errors")) == 0).drop("_cde_errors")
    
    df_error_raw = df_checked.filter(F.size(F.col("_cde_errors")) > 0)
    
    # Format the error DataFrame uniformly for an `errors_table`
    # Converts arrays to a single human-readable string: "Missing CDE: amount, Missing CDE: id"
    if df_error_raw.count() > 0:
        errors_df = df_error_raw.select(
            F.to_json(F.struct("*")).alias("raw_payload"),
            F.lit("CDE_VALIDATION_FAILED").alias("error_type"),
            F.array_join(F.col("_cde_errors"), ", ").alias("error_message"),
            F.current_timestamp().alias("processed_ts")
        )
    else:
        # Create an empty df with string columns to prevent schema mismatch on return
        errors_df = df_error_raw.select(
            F.lit("").alias("raw_payload"),
            F.lit("").alias("error_type"),
            F.lit("").alias("error_message"),
            F.current_timestamp().alias("processed_ts")
        ).limit(0)
    
    logger.info(f"Validation complete. Valid: {df_valid.count()}, Invalid: {df_error_raw.count()}")
    return df_valid, errors_df


def route_errors_to_table(errors_df: DataFrame, target_table: str):
    """
    Appends the errors_df to the specified Iceberg errors table.
    """
    record_count = errors_df.count()
    if record_count == 0:
        return
        
    logger.warning(f"Routing {record_count} error records to {target_table}")
    try:
        errors_df.writeTo(target_table).append()
        logger.info(f"Successfully wrote {record_count} errors to {target_table}")
    except Exception as e:
        logger.error(f"Failed to write errors to {target_table}: {e}")
