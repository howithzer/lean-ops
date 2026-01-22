"""
Shared Lambda: SQS → Firehose

This Lambda processes messages from MULTIPLE SQS queues and forwards them to a 
SINGLE shared Firehose stream. The topic_name is extracted from the SQS queue ARN
and included in the record for downstream routing.

Architecture:
    SQS-A ──┐
    SQS-B ──┼──→ THIS LAMBDA ──→ SHARED FIREHOSE ──→ Multiple Iceberg Tables
    SQS-C ──┘

The Firehose Transform Lambda will use topic_name to route to the correct table.
"""

import json
import os
import time
from datetime import datetime, timezone
from uuid import uuid4

# Add common library to path
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.topic_utils import extract_topic_from_arn
from common.error_classification import classify_error, ErrorAction
from common.aws_clients import get_firehose_client

# Environment variable
FIREHOSE_STREAM_NAME = os.environ.get('FIREHOSE_STREAM_NAME')


def extract_nested_value(data, *paths):
    """Extract a value from nested dict using multiple possible paths."""
    for path in paths:
        value = data
        try:
            for key in path:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    value = None
                    break
            if value is not None:
                return value
        except (KeyError, TypeError):
            continue
    return None


def extract_business_keys(payload):
    """Extract business keys from the payload for deduplication."""
    if not isinstance(payload, dict):
        return None, None, None
    
    idempotency_key = extract_nested_value(
        payload,
        ('_metadata', 'idempotencyKeyResource'),
        ('_metadata', 'idempotency_key'),
        ('idempotencyKeyResource',),
        ('idempotency_key',),
    )
    
    period_reference = extract_nested_value(
        payload,
        ('_metadata', 'periodReference'),
        ('periodReference',),
        ('period_reference',),
    )
    
    correlation_id = extract_nested_value(
        payload,
        ('_metadata', 'correlationId'),
        ('correlationId',),
        ('correlation_id',),
    )
    
    return idempotency_key, period_reference, correlation_id


def lambda_handler(event, context):
    """
    Process SQS messages from multiple queues and forward to shared Firehose.
    
    Key feature: Extracts topic_name from SQS queue ARN and includes it in the
    record for dynamic routing by the Firehose Transform Lambda.
    """
    records = []
    batch_item_failures = []
    dropped_count = 0
    
    # Group records by topic for logging
    topic_counts = {}
    
    for i, record in enumerate(event.get('Records', [])):
        try:
            # Extract topic from SQS queue ARN
            queue_arn = record.get('eventSourceARN', '')
            topic_name = extract_topic_from_arn(queue_arn)
            topic_counts[topic_name] = topic_counts.get(topic_name, 0) + 1
            
            # Parse SQS message body
            body = json.loads(record['body'])
            
            # Extract envelope fields from GCP Pub/Sub format
            if 'messageId' in body:
                message_id = body['messageId']
                publish_time = body.get('publishTime', datetime.now(timezone.utc).isoformat())
                payload = body.get('payload', body)
            else:
                message_id = body.get('message_id', str(uuid4()))
                publish_time = body.get('publish_time', datetime.now(timezone.utc).isoformat())
                payload = body
            
            # Extract business keys for deduplication
            idempotency_key, period_reference, correlation_id = extract_business_keys(payload)
            
            # Build Firehose record WITH topic_name for routing
            # NOTE: idempotency_key is NOT defaulted to message_id here.
            # This allows null values through for CDE validation testing.
            # The Standardized layer flattening will map _metadata.idempotencyKeyResource → idempotency_key
            firehose_record = {
                'topic_name': topic_name,  # ← KEY: Used by Firehose Transform for routing
                'message_id': message_id,
                'idempotency_key': idempotency_key,  # Preserve null for CDE testing
                'period_reference': period_reference,
                'correlation_id': correlation_id,
                'publish_time': publish_time,
                'ingestion_ts': int(time.time()),
                'json_payload': json.dumps(payload) if isinstance(payload, dict) else str(payload)
            }
            
            records.append({
                'Data': json.dumps(firehose_record) + '\n'
            })
            
        except Exception as e:
            classification = classify_error(e)
            
            if classification.action == ErrorAction.DROP:
                # Log and continue - don't retry malformed data
                print(f"DROPPING record {i} ({classification.error_type}): {e}")
                dropped_count += 1
                # Return SUCCESS for this record (don't add to failures)
            else:
                # RETRY - add to batch failures for SQS retry
                print(f"RETRY record {i} ({classification.error_type}): {e}")
                batch_item_failures.append({
                    'itemIdentifier': record['messageId']
                })
    
    # Log topic distribution
    print(f"Processing {len(records)} records from topics: {topic_counts}")
    
    # Send to shared Firehose in batches of 500
    if records:
        for i in range(0, len(records), 500):
            batch = records[i:i + 500]
            try:
                firehose = get_firehose_client()
                response = firehose.put_record_batch(
                    DeliveryStreamName=FIREHOSE_STREAM_NAME,
                    Records=batch
                )
                
                if response.get('FailedPutCount', 0) > 0:
                    print(f"Failed to deliver {response['FailedPutCount']} records")
                    for j, result in enumerate(response.get('RequestResponses', [])):
                        if 'ErrorCode' in result:
                            original_idx = i + j
                            if original_idx < len(event['Records']):
                                batch_item_failures.append({
                                    'itemIdentifier': event['Records'][original_idx]['messageId']
                                })
                                
            except Exception as e:
                print(f"Error sending batch to Firehose: {e}")
                for j in range(len(batch)):
                    original_idx = i + j
                    if original_idx < len(event['Records']):
                        batch_item_failures.append({
                            'itemIdentifier': event['Records'][original_idx]['messageId']
                        })
    
    print(f"Processed {len(records)} records, {len(batch_item_failures)} failures")
    
    return {
        'batchItemFailures': batch_item_failures
    }
