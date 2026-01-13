"""
Update Checkpoint Lambda Handler
=================================
Updates DynamoDB checkpoint after successful Glue job execution.
Called by Step Function after RunCurationJob completes.
"""

import json
import os
from datetime import datetime
import boto3

# Environment variables
CHECKPOINT_TABLE = os.environ.get('CHECKPOINT_TABLE', 'lean-ops-dev-checkpoints')

# Lazy-loaded clients
_dynamodb = None


def get_dynamodb():
    """Get DynamoDB resource with lazy initialization."""
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb')
    return _dynamodb


def lambda_handler(event, context):
    """
    Update checkpoint in DynamoDB.
    
    Expected event:
    {
        "topic_name": "events",
        "layer": "curated",           # or "semantic"
        "checkpoint_value": 1234567890123
    }
    
    Returns:
    {
        "success": true,
        "topic_name": "events",
        "layer": "curated",
        "checkpoint_value": 1234567890123
    }
    """
    print(f"Event: {json.dumps(event)}")
    
    topic_name = event.get('topic_name')
    layer = event.get('layer', 'curated')
    checkpoint_value = event.get('checkpoint_value')
    
    if not topic_name or checkpoint_value is None:
        raise ValueError("topic_name and checkpoint_value are required")
    
    # Get table
    table = get_dynamodb().Table(CHECKPOINT_TABLE)
    
    # Build checkpoint key based on layer
    checkpoint_key = f"{layer}_checkpoint"
    
    # Update checkpoint
    table.update_item(
        Key={'topic_name': topic_name},
        UpdateExpression=f"SET {checkpoint_key} = :val, updated_at = :ts",
        ExpressionAttributeValues={
            ':val': checkpoint_value,
            ':ts': datetime.utcnow().isoformat()
        }
    )
    
    print(f"Checkpoint updated: {topic_name}/{layer} -> {checkpoint_value}")
    
    return {
        'success': True,
        'topic_name': topic_name,
        'layer': layer,
        'checkpoint_value': checkpoint_value
    }
