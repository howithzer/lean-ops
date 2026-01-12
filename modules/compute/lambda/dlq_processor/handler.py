"""
DLQ Processor Lambda: Centralized DLQ → S3 Archive + DynamoDB

This Lambda processes messages from the centralized Dead Letter Queue and:
1. Extracts the original topic from the source queue ARN
2. Archives the message to S3 (prefixed by topic/date)
3. Logs to DynamoDB error_tracker for visibility
4. Enables replay by preserving the original message

Architecture:
    Centralized DLQ → THIS LAMBDA → S3 (dlq-archive/)
                                  → DynamoDB (error_tracker)
"""

import json
import os
from datetime import datetime, timezone
from uuid import uuid4

# Add common library to path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.topic_utils import extract_topic_from_dlq_attributes
from common.error_classification import classify_dlq_error
from common.aws_clients import get_s3_client, get_dynamodb_resource

# Environment variables
ARCHIVE_BUCKET = os.environ.get('ARCHIVE_BUCKET')
ERROR_TABLE = os.environ.get('ERROR_TABLE')


def archive_to_s3(topic, message_id, body, attributes, receive_count):
    """Archive failed message to S3 for later analysis/replay."""
    
    now = datetime.now(timezone.utc)
    date_prefix = now.strftime('%Y-%m-%d')
    hour = now.strftime('%H')
    
    # S3 key: dlq-archive/{topic}/{date}/{hour}/{message_id}.json
    s3_key = f"dlq-archive/{topic}/{date_prefix}/{hour}/{message_id}.json"
    
    archive_record = {
        'message_id': message_id,
        'topic': topic,
        'body': body,
        'attributes': attributes,
        'receive_count': receive_count,
        'archived_at': now.isoformat(),
        'replay_eligible': True
    }
    
    s3 = get_s3_client()
    s3.put_object(
        Bucket=ARCHIVE_BUCKET,
        Key=s3_key,
        Body=json.dumps(archive_record, indent=2),
        ContentType='application/json'
    )
    
    return s3_key


def log_to_dynamodb(topic, message_id, error_type, s3_key, body_preview):
    """Log error to DynamoDB for operational visibility."""
    
    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(ERROR_TABLE)
    now = datetime.now(timezone.utc)
    
    table.put_item(Item={
        'topic_name': topic,
        'timestamp_message_id': f"{now.isoformat()}#{message_id}",
        'error_type': error_type,
        's3_archive_key': s3_key,
        'body_preview': body_preview[:500] if body_preview else '',
        'timestamp': int(now.timestamp()),
        'ttl': int(now.timestamp()) + (90 * 24 * 60 * 60)  # 90 days TTL
    })





def lambda_handler(event, context):
    """
    Process messages from centralized DLQ.
    
    For each message:
    1. Extract original topic
    2. Archive to S3
    3. Log to DynamoDB
    """
    
    processed = 0
    errors = 0
    
    for record in event.get('Records', []):
        try:
            message_id = record.get('messageId', str(uuid4()))
            body = record.get('body', '')
            attributes = record.get('attributes', {})
            receive_count = int(attributes.get('ApproximateReceiveCount', 0))
            
            # Extract original topic
            topic = extract_topic_from_dlq_attributes(attributes)
            
            # Classify why it ended up in DLQ
            error_type = classify_dlq_error(body)
            
            # Archive to S3
            s3_key = archive_to_s3(topic, message_id, body, attributes, receive_count)
            
            # Log to DynamoDB
            log_to_dynamodb(topic, message_id, error_type, s3_key, body)
            
            print(f"Archived DLQ message: topic={topic}, id={message_id}, type={error_type}")
            processed += 1
            
        except Exception as e:
            print(f"Error processing DLQ message: {e}")
            errors += 1
    
    print(f"DLQ Processor: {processed} archived, {errors} errors")
    
    return {
        'processed': processed,
        'errors': errors
    }
