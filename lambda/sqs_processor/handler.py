"""
Lean-Ops: SQS Processor Lambda

Shared Lambda that processes messages from MULTIPLE SQS queues and forwards
them to a SINGLE shared Firehose stream.

Features:
- Error classification (Drop vs Retry vs DLQ)
- Topic extraction from SQS queue ARN
- ReportBatchItemFailures for partial batch handling
- Enriched error logging to Error Tracker (DynamoDB)
- All dropped messages still go to DLQ for archival

Architecture:
    SQS-A ──┐                              ┌──→ Firehose
    SQS-B ──┼──→ THIS LAMBDA ──┬───────────┤
    SQS-N ──┘                  │           └──→ DLQ (enriched)
                               │
                               └──→ Error Tracker (DynamoDB)
"""

import json
import os
import boto3
import time
from datetime import datetime, timezone
from uuid import uuid4

# Initialize clients
firehose = boto3.client('firehose')
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')

# Environment variables
FIREHOSE_STREAM_NAME = os.environ.get('FIREHOSE_STREAM_NAME')
DLQ_URL = os.environ.get('DLQ_URL')
ERROR_TRACKER_TABLE = os.environ.get('ERROR_TRACKER_TABLE')

error_tracker = dynamodb.Table(ERROR_TRACKER_TABLE) if ERROR_TRACKER_TABLE else None


# =============================================================================
# ERROR CLASSIFICATION
# =============================================================================

class ErrorType:
    MALFORMED_JSON = 'MALFORMED_JSON'
    INVALID_CONTRACT = 'INVALID_CONTRACT'
    FIREHOSE_THROTTLE = 'FIREHOSE_THROTTLE'
    DOWNSTREAM_TIMEOUT = 'DOWNSTREAM_TIMEOUT'
    UNKNOWN = 'UNKNOWN'


def classify_error(error: Exception) -> str:
    """Classify error to determine handling strategy."""
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    if 'json' in error_str or error_type == 'JSONDecodeError':
        return ErrorType.MALFORMED_JSON
    elif 'contract' in error_str or 'schema' in error_str:
        return ErrorType.INVALID_CONTRACT
    elif 'throttl' in error_str or 'limit' in error_str:
        return ErrorType.FIREHOSE_THROTTLE
    elif 'timeout' in error_str:
        return ErrorType.DOWNSTREAM_TIMEOUT
    else:
        return ErrorType.UNKNOWN


def should_retry(error_type: str) -> bool:
    """Determine if error type should be retried."""
    non_retryable = {ErrorType.MALFORMED_JSON, ErrorType.INVALID_CONTRACT}
    return error_type not in non_retryable


# =============================================================================
# TOPIC EXTRACTION
# =============================================================================

def extract_topic_from_queue_arn(queue_arn: str) -> str:
    """
    Extract topic name from SQS queue ARN.
    
    Example:
        arn:aws:sqs:us-east-1:123456789012:lean-ops-dev-events-queue
        → events
    """
    queue_name = queue_arn.split(':')[-1]
    # Pattern: lean-ops-{env}-{topic}-queue
    parts = queue_name.replace('-queue', '').split('-')
    return parts[-1] if len(parts) >= 3 else queue_name


# =============================================================================
# ERROR HANDLING
# =============================================================================

def log_to_error_tracker(record: dict, topic: str, error_type: str, 
                         action: str, error_message: str):
    """Log error to DynamoDB Error Tracker."""
    if not error_tracker:
        print(f"[ERROR_TRACKER] No table configured, skipping log")
        return
    
    try:
        error_tracker.put_item(Item={
            'topic_name': topic,
            'timestamp_message_id': f"{datetime.now(timezone.utc).isoformat()}#{record.get('messageId', 'unknown')}",
            'error_type': error_type,
            'action_taken': action,
            'error_message': error_message,
            'receive_count': int(record.get('attributes', {}).get('ApproximateReceiveCount', 1)),
            'correlation_id': extract_correlation_id(record),
            'resolved': False,
            'created_at': datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        print(f"[ERROR_TRACKER] Failed to log: {e}")


def send_to_dlq_with_context(record: dict, topic: str, error_type: str, 
                             action: str, error_message: str):
    """Send enriched message to centralized DLQ."""
    if not DLQ_URL:
        print(f"[DLQ] No URL configured, skipping")
        return
    
    enriched_message = {
        'topic_name': topic,
        'source_queue': record.get('eventSourceARN', 'unknown'),
        'message_id': record.get('messageId', 'unknown'),
        'correlation_id': extract_correlation_id(record),
        'error_type': error_type,
        'action_taken': action,
        'error_message': error_message,
        'receive_count': int(record.get('attributes', {}).get('ApproximateReceiveCount', 1)),
        'first_received_at': record.get('attributes', {}).get('SentTimestamp'),
        'dlq_sent_at': datetime.now(timezone.utc).isoformat(),
        'original_body': record.get('body', '')
    }
    
    try:
        sqs.send_message(
            QueueUrl=DLQ_URL,
            MessageBody=json.dumps(enriched_message),
            MessageAttributes={
                'topic_name': {'DataType': 'String', 'StringValue': topic},
                'error_type': {'DataType': 'String', 'StringValue': error_type}
            }
        )
    except Exception as e:
        print(f"[DLQ] Failed to send: {e}")


def extract_correlation_id(record: dict) -> str:
    """Extract correlation_id from record body if present."""
    try:
        body = json.loads(record.get('body', '{}'))
        return (body.get('correlation_id') or 
                body.get('_metadata', {}).get('correlationId') or
                'unknown')
    except:
        return 'unknown'


# =============================================================================
# RECORD PROCESSING
# =============================================================================

def extract_business_keys(payload: dict) -> tuple:
    """Extract business keys for deduplication."""
    if not isinstance(payload, dict):
        return None, None, None
    
    def get_nested(obj, *paths):
        for path in paths:
            val = obj
            try:
                for key in path:
                    val = val.get(key) if isinstance(val, dict) else None
                if val:
                    return val
            except:
                continue
        return None
    
    idempotency_key = get_nested(
        payload,
        ('_metadata', 'idempotencyKeyResource'),
        ('idempotency_key',),
    )
    period_reference = get_nested(
        payload,
        ('_metadata', 'periodReference'),
        ('period_reference',),
    )
    correlation_id = get_nested(
        payload,
        ('_metadata', 'correlationId'),
        ('correlation_id',),
    )
    
    return idempotency_key, period_reference, correlation_id


def process_record(record: dict, topic: str) -> dict:
    """
    Process a single SQS record and return Firehose record.
    
    Raises exceptions that will be classified by the handler.
    """
    # Parse body (may raise JSONDecodeError)
    body = json.loads(record['body'])
    
    # Extract envelope fields
    if 'messageId' in body:
        message_id = body['messageId']
        publish_time = body.get('publishTime', datetime.now(timezone.utc).isoformat())
        payload = body.get('payload', body)
    else:
        message_id = body.get('message_id', str(uuid4()))
        publish_time = body.get('publish_time', datetime.now(timezone.utc).isoformat())
        payload = body
    
    # Extract business keys
    idempotency_key, period_reference, correlation_id = extract_business_keys(payload)
    
    # Build Firehose record with topic_name for routing
    return {
        'topic_name': topic,
        'message_id': message_id,
        'idempotency_key': idempotency_key or message_id,
        'period_reference': period_reference,
        'correlation_id': correlation_id,
        'publish_time': publish_time,
        'ingestion_ts': int(time.time()),
        'json_payload': json.dumps(payload) if isinstance(payload, dict) else str(payload)
    }


# =============================================================================
# MAIN HANDLER
# =============================================================================

def lambda_handler(event, context):
    """
    Process SQS messages from multiple queues and forward to shared Firehose.
    
    Error handling strategy:
    - MALFORMED_JSON: Drop (no retry), send to DLQ immediately
    - INVALID_CONTRACT: Drop (no retry), send to DLQ immediately
    - FIREHOSE_THROTTLE: Retry via SQS
    - UNKNOWN: Retry via SQS
    """
    records_to_send = []
    batch_item_failures = []
    
    # Track stats
    stats = {'processed': 0, 'dropped': 0, 'retryable': 0, 'by_topic': {}}
    
    for record in event.get('Records', []):
        topic = extract_topic_from_queue_arn(record.get('eventSourceARN', ''))
        stats['by_topic'][topic] = stats['by_topic'].get(topic, 0) + 1
        receive_count = int(record.get('attributes', {}).get('ApproximateReceiveCount', 1))
        
        try:
            firehose_record = process_record(record, topic)
            records_to_send.append({
                'Data': json.dumps(firehose_record) + '\n'
            })
            stats['processed'] += 1
            
        except Exception as e:
            error_type = classify_error(e)
            error_message = str(e)
            
            if should_retry(error_type):
                # Retryable error: report failure, SQS will retry
                action = f'RETRY_{receive_count}'
                log_to_error_tracker(record, topic, error_type, action, error_message)
                batch_item_failures.append({'itemIdentifier': record['messageId']})
                stats['retryable'] += 1
            else:
                # Non-retryable: send to DLQ immediately, report success to SQS
                action = 'DROPPED_NO_RETRY'
                log_to_error_tracker(record, topic, error_type, action, error_message)
                send_to_dlq_with_context(record, topic, error_type, action, error_message)
                stats['dropped'] += 1
    
    # Send to Firehose in batches of 500
    if records_to_send:
        for i in range(0, len(records_to_send), 500):
            batch = records_to_send[i:i + 500]
            try:
                response = firehose.put_record_batch(
                    DeliveryStreamName=FIREHOSE_STREAM_NAME,
                    Records=batch
                )
                
                if response.get('FailedPutCount', 0) > 0:
                    print(f"[FIREHOSE] Failed to deliver {response['FailedPutCount']} records")
                    for j, result in enumerate(response.get('RequestResponses', [])):
                        if 'ErrorCode' in result:
                            original_idx = i + j
                            if original_idx < len(event['Records']):
                                batch_item_failures.append({
                                    'itemIdentifier': event['Records'][original_idx]['messageId']
                                })
                                
            except Exception as e:
                print(f"[FIREHOSE] Batch error: {e}")
                # Mark entire batch for retry
                for j in range(len(batch)):
                    original_idx = i + j
                    if original_idx < len(event['Records']):
                        batch_item_failures.append({
                            'itemIdentifier': event['Records'][original_idx]['messageId']
                        })
    
    print(f"[STATS] {json.dumps(stats)}")
    
    return {'batchItemFailures': batch_item_failures}
