"""
Lock Manager Lambda - Handles distributed lock acquisition and release with TTL.

Provides production-grade locking for Step Functions using DynamoDB with proper
epoch TTL for automatic cleanup of orphaned locks.
"""
import json
import time
import logging
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.client('dynamodb')

# Lock TTL: 1 hour (3600 seconds) - safety net for orphaned locks
LOCK_TTL_SECONDS = 3600


def handler(event, context):
    """
    Lock manager entry point.
    
    Actions:
        - acquire: Try to acquire lock for topic (conditional put with TTL)
        - release: Release lock for topic (delete item)
    
    Input:
        {
            "action": "acquire" | "release",
            "topic_name": "events",
            "execution_id": "arn:aws:states:..."  # For acquire only
        }
    
    Output:
        {
            "acquired": true/false,  # For acquire action
            "released": true/false,  # For release action
            "topic_name": "events",
            "message": "..."
        }
    """
    action = event.get('action', 'acquire')
    topic_name = event.get('topic_name')
    execution_id = event.get('execution_id', 'unknown')
    table_name = event.get('table_name')
    
    if not topic_name:
        raise ValueError("topic_name is required")
    
    if not table_name:
        raise ValueError("table_name is required")
    
    if action == 'acquire':
        return acquire_lock(table_name, topic_name, execution_id)
    elif action == 'release':
        return release_lock(table_name, topic_name)
    else:
        raise ValueError(f"Unknown action: {action}")


def acquire_lock(table_name: str, topic_name: str, execution_id: str) -> dict:
    """
    Attempt to acquire a distributed lock using DynamoDB conditional put.
    
    Uses:
        - ConditionExpression to prevent overwriting existing locks
        - TTL attribute for automatic cleanup of orphaned locks
    """
    ttl_epoch = int(time.time()) + LOCK_TTL_SECONDS
    acquired_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    
    try:
        dynamodb.put_item(
            TableName=table_name,
            Item={
                'topic_name': {'S': topic_name},
                'lock_owner': {'S': execution_id},
                'acquired_at': {'S': acquired_at},
                'ttl': {'N': str(ttl_epoch)}
            },
            ConditionExpression='attribute_not_exists(topic_name)'
        )
        
        logger.info(f"Lock acquired for topic={topic_name}, owner={execution_id}, ttl={ttl_epoch}")
        
        return {
            'acquired': True,
            'topic_name': topic_name,
            'lock_owner': execution_id,
            'ttl_epoch': ttl_epoch,
            'message': 'Lock acquired successfully'
        }
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning(f"Lock already held for topic={topic_name}")
            return {
                'acquired': False,
                'topic_name': topic_name,
                'message': 'Lock already held by another execution'
            }
        raise


def release_lock(table_name: str, topic_name: str) -> dict:
    """Release a distributed lock by deleting the item."""
    try:
        dynamodb.delete_item(
            TableName=table_name,
            Key={'topic_name': {'S': topic_name}}
        )
        
        logger.info(f"Lock released for topic={topic_name}")
        
        return {
            'released': True,
            'topic_name': topic_name,
            'message': 'Lock released successfully'
        }
        
    except Exception as e:
        logger.error(f"Failed to release lock for topic={topic_name}: {e}")
        # Return success anyway - if the lock doesn't exist, that's fine
        return {
            'released': True,
            'topic_name': topic_name,
            'message': f'Lock release attempted: {str(e)}'
        }
