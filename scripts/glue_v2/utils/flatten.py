"""
JSON Flatten Utilities for Curated Processor
=============================================
Provides deep JSON flattening with configurable depth and envelope exclusion.

Pure Python functions (recursive_flatten, sanitize_column_name) can be used
without Spark. Spark-dependent functions require pyspark at runtime.
"""

import json
from typing import List, Tuple, Set, Optional, Any

# Try to import PySpark - will succeed in Glue, may fail in local testing
try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    SPARK_AVAILABLE = True
except ImportError:
    # PySpark not installed - pure Python functions still work
    DataFrame = None  # type: ignore
    SPARK_AVAILABLE = False

from .config import (
    get_logger,
    ENVELOPE_COLUMN_VARIANTS,
    MAX_FLATTEN_DEPTH,
    FLATTEN_SAMPLE_SIZE,
)

logger = get_logger(__name__)


def recursive_flatten(
    obj: Any,
    prefix: str = '',
    depth: int = 0,
    max_depth: int = MAX_FLATTEN_DEPTH
) -> List[Tuple[str, Optional[str]]]:
    """
    Recursively flatten a dictionary into a list of (key, value) tuples.
    
    Nested keys become underscore-separated column names:
    - event.userId -> event_userid
    - event.metadata.ipAddress -> event_metadata_ipaddress
    
    Args:
        obj: The object to flatten (dict, list, or primitive)
        prefix: Current key prefix for nested keys
        depth: Current recursion depth
        max_depth: Maximum depth to recurse (default from config)
    
    Returns:
        List of (key, value) tuples where keys are flattened column names
    
    Example:
        >>> recursive_flatten({"user": {"id": "123", "name": "John"}})
        [('user_id', '123'), ('user_name', 'John')]
    """
    items: List[Tuple[str, Optional[str]]] = []
    
    if depth >= max_depth:
        # Max depth reached - store as JSON string
        value = json.dumps(obj) if isinstance(obj, (dict, list)) else obj
        items.append((prefix, value))
        return items
    
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{prefix}_{k}" if prefix else k
            new_key = sanitize_column_name(new_key)
            
            if isinstance(v, dict):
                items.extend(recursive_flatten(v, new_key, depth + 1, max_depth))
            elif isinstance(v, list):
                # Store lists as JSON strings
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, str(v) if v is not None else None))
    else:
        items.append((prefix, str(obj) if obj is not None else None))
    
    return items


def sanitize_column_name(name: str) -> str:
    """
    Sanitize a column name for use in Spark/Iceberg.
    
    - Replaces dashes and dots with underscores
    - Converts to lowercase
    
    Args:
        name: Raw column name
    
    Returns:
        Sanitized column name safe for SQL operations
    """
    return name.replace("-", "_").replace(".", "_").lower()


def recursive_flatten_with_paths(
    obj: Any,
    prefix: str = '',
    json_path: str = '$',
    depth: int = 0,
    max_depth: int = MAX_FLATTEN_DEPTH
) -> List[Tuple[str, str, Optional[str]]]:
    """
    Recursively flatten a dictionary, returning (column_name, json_path, value) tuples.
    
    This version tracks the JSONPath for use with Spark's get_json_object().
    
    Args:
        obj: The object to flatten (dict, list, or primitive)
        prefix: Current key prefix for column names
        json_path: Current JSONPath for get_json_object
        depth: Current recursion depth
        max_depth: Maximum depth to recurse
    
    Returns:
        List of (column_name, json_path, value) tuples
    """
    items: List[Tuple[str, str, Optional[str]]] = []
    
    if depth >= max_depth:
        value = json.dumps(obj) if isinstance(obj, (dict, list)) else obj
        items.append((prefix, json_path, value))
        return items
    
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_col = f"{prefix}_{k}" if prefix else k
            new_col = sanitize_column_name(new_col)
            new_path = f"{json_path}.{k}"
            
            if isinstance(v, dict):
                items.extend(recursive_flatten_with_paths(v, new_col, new_path, depth + 1, max_depth))
            elif isinstance(v, list):
                items.append((new_col, new_path, json.dumps(v)))
            else:
                items.append((new_col, new_path, str(v) if v is not None else None))
    else:
        items.append((prefix, json_path, str(obj) if obj is not None else None))
    
    return items


def discover_payload_keys_with_paths(
    df: DataFrame,
    sample_size: int = FLATTEN_SAMPLE_SIZE,
    max_depth: int = MAX_FLATTEN_DEPTH
) -> dict:
    """
    Discover all unique keys from json_payload column by sampling rows.
    
    Returns a dict mapping column_name -> json_path for use with get_json_object.
    
    Args:
        df: DataFrame with json_payload column
        sample_size: Number of rows to sample for key discovery
        max_depth: Maximum depth for recursive flattening
    
    Returns:
        Dict of {column_name: json_path} for each discovered key
    """
    logger.info("Discovering payload keys from %d sample rows...", sample_size)
    
    sample_rows = df.select("json_payload").limit(sample_size).collect()
    key_to_path: dict = {}
    
    for row in sample_rows:
        if row.json_payload:
            try:
                payload_dict = json.loads(row.json_payload)
                flattened = recursive_flatten_with_paths(payload_dict, '', '$', 0, max_depth)
                for col_name, json_path, _ in flattened:
                    if col_name and col_name not in key_to_path:
                        key_to_path[col_name] = json_path
            except json.JSONDecodeError:
                continue
    
    logger.info("Discovered %d unique keys: %s...", len(key_to_path), sorted(key_to_path.keys())[:20])
    return key_to_path


# Keep old function for backward compatibility
def discover_payload_keys(
    df: DataFrame,
    sample_size: int = FLATTEN_SAMPLE_SIZE,
    max_depth: int = MAX_FLATTEN_DEPTH
) -> Set[str]:
    """Backward compatible wrapper."""
    return set(discover_payload_keys_with_paths(df, sample_size, max_depth).keys())


def create_key_extractor(target_key: str, max_depth: int = MAX_FLATTEN_DEPTH):
    """
    Create a Spark UDF to extract a specific key from json_payload.
    
    Args:
        target_key: The flattened key to extract
        max_depth: Maximum depth for recursive flattening
    
    Returns:
        Spark UDF that extracts the key value as STRING
    
    Raises:
        ImportError: If pyspark is not installed
    """
    
    def extract_key(json_str: Optional[str]) -> Optional[str]:
        if not json_str:
            return None
        try:
            payload = json.loads(json_str)
            flattened = dict(recursive_flatten(payload, '', 0, max_depth))
            return flattened.get(target_key)
        except Exception:
            return None
    
    return F.udf(extract_key, StringType())


def flatten_json_payload(
    df: DataFrame,
    max_depth: int = MAX_FLATTEN_DEPTH,
    exclude_columns: Optional[Set[str]] = None
) -> DataFrame:
    """
    Deep flatten json_payload column into individual STRING columns.
    
    Uses native Spark SQL get_json_object() instead of Python UDFs
    to avoid Tungsten codegen serialization issues.
    
    Args:
        df: DataFrame with json_payload column
        max_depth: Maximum nesting depth to flatten (default 5)
        exclude_columns: Additional columns to exclude from extraction
    
    Returns:
        DataFrame with flattened columns (json_payload dropped)
    """
    logger.info("Starting deep flatten (max_depth=%d) using native Spark SQL...", max_depth)
    
    # Discover all keys with their JSON paths
    key_to_path = discover_payload_keys_with_paths(df, FLATTEN_SAMPLE_SIZE, max_depth)
    
    # Build exclusion set: existing columns + envelope variants + custom exclusions
    existing_cols = set(c.lower() for c in df.columns)
    exclude_set = existing_cols.union(ENVELOPE_COLUMN_VARIANTS)
    
    if exclude_columns:
        exclude_set = exclude_set.union(exclude_columns)
    
    # Log which envelope columns are being excluded
    excluded_envelope = ENVELOPE_COLUMN_VARIANTS & set(key_to_path.keys())
    if excluded_envelope:
        logger.info("Excluding envelope column variants: %s", sorted(excluded_envelope))
    
    # Add each discovered key as a column using native get_json_object
    added_count = 0
    for col_name in sorted(key_to_path.keys()):
        safe_col = sanitize_column_name(col_name)
        
        if safe_col in exclude_set:
            logger.debug("Skipping '%s' (excluded)", safe_col)
            continue
        
        json_path = key_to_path[col_name]
        # Use native Spark SQL get_json_object - no Python UDF!
        df = df.withColumn(safe_col, F.get_json_object(F.col("json_payload"), json_path))
        logger.debug("Added column '%s' from path '%s'", safe_col, json_path)
        added_count += 1
    
    logger.info("Deep flatten complete: added %d new columns (using native Spark SQL)", added_count)
    
    # Drop original json_payload
    return df.drop("json_payload")


# Legacy UDF-based function (DEPRECATED - causes Tungsten codegen issues)
def create_key_extractor(target_key: str, max_depth: int = MAX_FLATTEN_DEPTH):
    """
    DEPRECATED: Use get_json_object in flatten_json_payload instead.
    
    This UDF-based approach causes 'Cannot generate code for expression'
    errors in Spark's Tungsten code generation.
    """
    import warnings
    warnings.warn(
        "create_key_extractor is deprecated due to Tungsten codegen issues. "
        "Use flatten_json_payload with native get_json_object instead.",
        DeprecationWarning
    )
    
    def extract_key(json_str: Optional[str]) -> Optional[str]:
        if not json_str:
            return None
        try:
            payload = json.loads(json_str)
            flattened = dict(recursive_flatten(payload, '', 0, max_depth))
            return flattened.get(target_key)
        except Exception:
            return None
    
    return F.udf(extract_key, StringType())
