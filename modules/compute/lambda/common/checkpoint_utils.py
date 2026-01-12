"""
Checkpoint utilities for Step Function orchestration.

Provides functions to query Iceberg table snapshots and DynamoDB checkpoints
in a single call, optimized for the GetAllCheckpoints Lambda.
"""
import os
from dataclasses import dataclass
from typing import Optional
from .aws_clients import get_glue_client, get_dynamodb_client


@dataclass
class CheckpointData:
    """All checkpoint data for Step Function orchestration."""
    raw_snapshot: Optional[str]
    curated_checkpoint: Optional[str]
    semantic_checkpoint: Optional[str]
    schema_exists: bool


def get_iceberg_current_snapshot(database: str, table: str) -> Optional[str]:
    """
    Get the current snapshot ID of an Iceberg table from Glue catalog.
    
    Args:
        database: Glue database name
        table: Glue table name
    
    Returns:
        Snapshot ID string or None if table doesn't exist
    """
    glue = get_glue_client()
    
    try:
        response = glue.get_table(DatabaseName=database, Name=table)
        table_info = response.get('Table', {})
        
        # Iceberg metadata location contains snapshot info
        # For simplicity, we use the table's UpdateTime as a proxy
        # In production, you'd parse the metadata.json file
        parameters = table_info.get('Parameters', {})
        
        # Try to get current-snapshot-id from Iceberg table properties
        snapshot_id = parameters.get('current-snapshot-id')
        if snapshot_id:
            return snapshot_id
        
        # Fallback: use metadata_location as identifier
        return parameters.get('metadata_location', table_info.get('UpdateTime', ''))
        
    except glue.exceptions.EntityNotFoundException:
        return None
    except Exception as e:
        print(f"Error getting snapshot for {database}.{table}: {e}")
        return None


def get_dynamodb_checkpoint(table_name: str, topic: str, layer: str) -> Optional[str]:
    """
    Get the last processed checkpoint from DynamoDB.
    
    Args:
        table_name: DynamoDB table name (e.g., 'lean-ops-checkpoints')
        topic: Topic name (e.g., 'events')
        layer: Layer name ('curated' or 'semantic')
    
    Returns:
        Checkpoint value (snapshot ID or timestamp) or None
    """
    dynamodb = get_dynamodb_client()
    
    try:
        response = dynamodb.get_item(
            TableName=table_name,
            Key={
                'topic_name': {'S': topic},
                'layer': {'S': layer}
            }
        )
        
        item = response.get('Item', {})
        return item.get('checkpoint', {}).get('S')
        
    except Exception as e:
        print(f"Error getting checkpoint for {topic}/{layer}: {e}")
        return None


def check_schema_exists(database: str, table: str) -> bool:
    """
    Check if a Semantic table schema exists in Glue catalog.
    
    Args:
        database: Glue database name
        table: Semantic table name
    
    Returns:
        True if table exists and has columns defined
    """
    glue = get_glue_client()
    
    try:
        response = glue.get_table(DatabaseName=database, Name=table)
        columns = response.get('Table', {}).get('StorageDescriptor', {}).get('Columns', [])
        return len(columns) > 0
    except glue.exceptions.EntityNotFoundException:
        return False
    except Exception as e:
        print(f"Error checking schema for {database}.{table}: {e}")
        return False


def get_all_checkpoints(
    raw_database: str,
    raw_table: str,
    curated_database: str,
    curated_table: str,
    semantic_database: str,
    semantic_table: str,
    checkpoint_table: str,
    topic: str
) -> CheckpointData:
    """
    Get all checkpoints in a single call for Step Function optimization.
    
    This replaces multiple Lambda invocations with a single call that returns:
    - RAW table current snapshot
    - Curated layer last processed checkpoint
    - Semantic layer last processed checkpoint
    - Whether the Semantic schema exists
    
    Args:
        raw_database: Glue database for RAW tables
        raw_table: RAW table name (e.g., 'events_staging')
        curated_database: Glue database for Curated tables
        curated_table: Curated table name
        semantic_database: Glue database for Semantic tables
        semantic_table: Semantic table name
        checkpoint_table: DynamoDB checkpoint table name
        topic: Topic name for checkpoint lookup
    
    Returns:
        CheckpointData with all snapshot/checkpoint information
    """
    # Get current RAW snapshot
    raw_snapshot = get_iceberg_current_snapshot(raw_database, raw_table)
    
    # Get checkpoint offsets from DynamoDB
    curated_checkpoint = get_dynamodb_checkpoint(checkpoint_table, topic, 'curated')
    semantic_checkpoint = get_dynamodb_checkpoint(checkpoint_table, topic, 'semantic')
    
    # Check if Semantic schema is defined
    schema_exists = check_schema_exists(semantic_database, semantic_table)
    
    return CheckpointData(
        raw_snapshot=raw_snapshot,
        curated_checkpoint=curated_checkpoint,
        semantic_checkpoint=semantic_checkpoint,
        schema_exists=schema_exists
    )
