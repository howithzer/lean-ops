"""
GetAllCheckpoints Lambda: Single call for Step Function orchestration.

This Lambda replaces multiple checkpoint lookups with a single call that returns:
- RAW table current snapshot
- Curated layer last processed checkpoint
- Semantic layer last processed checkpoint
- Whether the Semantic schema exists

This optimization reduces Step Function cold starts and API calls.
"""
import os
import json

# Add common library to path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.checkpoint_utils import get_all_checkpoints


# Environment variables
RAW_DATABASE = os.environ.get('RAW_DATABASE', 'iceberg_raw_db')
CURATED_DATABASE = os.environ.get('CURATED_DATABASE', 'iceberg_curated_db')
SEMANTIC_DATABASE = os.environ.get('SEMANTIC_DATABASE', 'iceberg_semantic_db')
CHECKPOINT_TABLE = os.environ.get('CHECKPOINT_TABLE', 'lean-ops-checkpoints')


def lambda_handler(event, context):
    """
    Get all checkpoints for Step Function orchestration.
    
    Input:
    {
        "topic": "events"  # Optional, defaults to checking all registered topics
    }
    
    Output:
    {
        "raw_snapshot": "s3://bucket/iceberg/raw/metadata/snap-001.avro",
        "curated_checkpoint": "snap-000",
        "semantic_checkpoint": "snap-000",
        "schema_exists": true,
        "has_new_raw_data": true,
        "has_new_curated_data": true
    }
    """
    topic = event.get('topic', 'default')
    
    print(f"Getting checkpoints for topic: {topic}")
    
    # Build table names based on topic
    raw_table = f"{topic}_staging"
    curated_table = f"{topic}_curated"
    semantic_table = f"{topic}_semantic"
    
    # Get all checkpoints in one call
    checkpoints = get_all_checkpoints(
        raw_database=RAW_DATABASE,
        raw_table=raw_table,
        curated_database=CURATED_DATABASE,
        curated_table=curated_table,
        semantic_database=SEMANTIC_DATABASE,
        semantic_table=semantic_table,
        checkpoint_table=CHECKPOINT_TABLE,
        topic=topic
    )
    
    # Determine if there's new data to process
    has_new_raw_data = (
        checkpoints.raw_snapshot is not None and 
        checkpoints.raw_snapshot != checkpoints.curated_checkpoint
    )
    
    has_new_curated_data = (
        checkpoints.curated_checkpoint is not None and
        checkpoints.curated_checkpoint != checkpoints.semantic_checkpoint
    )
    
    result = {
        'topic': topic,
        'raw_snapshot': checkpoints.raw_snapshot,
        'curated_checkpoint': checkpoints.curated_checkpoint,
        'semantic_checkpoint': checkpoints.semantic_checkpoint,
        'schema_exists': checkpoints.schema_exists,
        'has_new_raw_data': has_new_raw_data,
        'has_new_curated_data': has_new_curated_data
    }
    
    print(f"Checkpoint result: {json.dumps(result)}")
    
    return result
