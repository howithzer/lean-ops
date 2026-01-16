"""
JSON Flatten Utilities for Curated Processor
=============================================
Provides deep JSON flattening with configurable depth and envelope exclusion.
"""

import json
from typing import List, Tuple, Set, Optional, Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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


def discover_payload_keys(
    df: DataFrame,
    sample_size: int = FLATTEN_SAMPLE_SIZE,
    max_depth: int = MAX_FLATTEN_DEPTH
) -> Set[str]:
    """
    Discover all unique keys from json_payload column by sampling rows.
    
    Args:
        df: DataFrame with json_payload column
        sample_size: Number of rows to sample for key discovery
        max_depth: Maximum depth for recursive flattening
    
    Returns:
        Set of unique flattened key names
    """
    logger.info("Discovering payload keys from %d sample rows...", sample_size)
    
    sample_rows = df.select("json_payload").limit(sample_size).collect()
    all_keys: Set[str] = set()
    
    for row in sample_rows:
        if row.json_payload:
            try:
                payload_dict = json.loads(row.json_payload)
                flattened = recursive_flatten(payload_dict, '', 0, max_depth)
                for key, _ in flattened:
                    if key:
                        all_keys.add(key)
            except json.JSONDecodeError:
                continue
    
    logger.info("Discovered %d unique keys: %s...", len(all_keys), sorted(all_keys)[:20])
    return all_keys


def create_key_extractor(target_key: str, max_depth: int = MAX_FLATTEN_DEPTH):
    """
    Create a Spark UDF to extract a specific key from json_payload.
    
    Args:
        target_key: The flattened key to extract
        max_depth: Maximum depth for recursive flattening
    
    Returns:
        Spark UDF that extracts the key value as STRING
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
    
    Recursively extracts nested keys up to max_depth levels.
    Nested keys become underscore-separated column names.
    
    Args:
        df: DataFrame with json_payload column
        max_depth: Maximum nesting depth to flatten (default 5)
        exclude_columns: Additional columns to exclude from extraction
    
    Returns:
        DataFrame with flattened columns (json_payload dropped)
    
    Example:
        Input:  {"json_payload": '{"user": {"id": "123"}}'}
        Output: {"user_id": "123"}
    """
    logger.info("Starting deep flatten (max_depth=%d)...", max_depth)
    
    # Discover all keys from sample
    all_keys = discover_payload_keys(df, FLATTEN_SAMPLE_SIZE, max_depth)
    
    # Build exclusion set: existing columns + envelope variants + custom exclusions
    existing_cols = set(c.lower() for c in df.columns)
    exclude_set = existing_cols.union(ENVELOPE_COLUMN_VARIANTS)
    
    if exclude_columns:
        exclude_set = exclude_set.union(exclude_columns)
    
    # Log which envelope columns are being excluded
    excluded_envelope = ENVELOPE_COLUMN_VARIANTS & all_keys
    if excluded_envelope:
        logger.info("Excluding envelope column variants: %s", sorted(excluded_envelope))
    
    # Add each discovered key as a column
    added_count = 0
    for key in sorted(all_keys):
        safe_key = sanitize_column_name(key)
        
        if safe_key in exclude_set:
            logger.debug("Skipping '%s' (excluded)", safe_key)
            continue
        
        extractor = create_key_extractor(key, max_depth)
        df = df.withColumn(safe_key, extractor(F.col("json_payload")))
        logger.debug("Added column '%s'", safe_key)
        added_count += 1
    
    logger.info("Deep flatten complete: added %d new columns", added_count)
    
    # Drop original json_payload
    return df.drop("json_payload")
