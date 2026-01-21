"""
Lambda: dlq_drainer
===================
Drains messages from the Centralized DLQ to S3 for analysis.
Triggered by EventBridge Schedule (e.g., hourly).

Strategy:
- Moving Window: Runs until 1 minute remains in timeout
- Safety: Deletes from SQS ONLY after successful S3 write
- Format: Writes line-delimited JSON files to S3
"""

import boto3
import json
import logging
import os
import uuid
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

DLQ_URL = os.environ['DLQ_URL']
TARGET_BUCKET = os.environ['TARGET_BUCKET']
TARGET_PREFIX = os.environ.get('TARGET_PREFIX', 'dlq_errors')
MAX_BATCH_SIZE = 10  # SQS Max
VISIBILITY_TIMEOUT = 300  # 5 mins

def lambda_handler(event, context):
    logger.info("Starting DLQ drainer...")
    
    total_processed = 0
    
    while True:
        # Check time remaining (leave 1 minute buffer)
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < 60000:
            logger.info("Time limit approaching (%d ms left). Stopping.", remaining_ms)
            break
            
        # Poll SQS
        response = sqs.receive_message(
            QueueUrl=DLQ_URL,
            MaxNumberOfMessages=MAX_BATCH_SIZE,
            WaitTimeSeconds=1,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )
        
        messages = response.get('Messages', [])
        if not messages:
            logger.info("Queue empty. Stopping.")
            break
            
        logger.info("Received batch of %d messages", len(messages))
        
        # Prepare batch for S3
        batch_id = str(uuid.uuid4())
        s3_key = f"{TARGET_PREFIX}/{datetime.utcnow().strftime('%Y/%m/%d')}/{batch_id}.json"
        
        json_lines = []
        entries_to_delete = []
        
        for msg in messages:
            # Extract useful metadata
            try:
                payload = {
                    "message_id": msg['MessageId'],
                    "receipt_handle": msg['ReceiptHandle'],
                    "body": msg['Body'],
                    "attributes": msg.get('Attributes', {}),
                    "message_attributes": msg.get('MessageAttributes', {}),
                    "drained_at": datetime.utcnow().isoformat()
                }
                
                # Check for error attributes (if injected by producer)
                if 'error_message' in payload['message_attributes']:
                    payload['error_cause'] = payload['message_attributes']['error_message']['StringValue']
                
                json_lines.append(json.dumps(payload))
                
                entries_to_delete.append({
                    'Id': msg['MessageId'],
                    'ReceiptHandle': msg['ReceiptHandle']
                })
            except Exception as e:
                logger.error("Error parsing message %s: %s", msg['MessageId'], e)
        
        if not json_lines:
            continue
            
        # Write to S3
        try:
            body_content = "\n".join(json_lines)
            s3.put_object(
                Bucket=TARGET_BUCKET,
                Key=s3_key,
                Body=body_content,
                ContentType='application/json'
            )
            logger.info("Wrote batch to s3://%s/%s", TARGET_BUCKET, s3_key)
            
            # Delete from SQS (Batch)
            del_resp = sqs.delete_message_batch(
                QueueUrl=DLQ_URL,
                Entries=entries_to_delete
            )
            
            successful_deletes = len(del_resp.get('Successful', []))
            failed_deletes = len(del_resp.get('Failed', []))
            
            logger.info("Deleted %d messages (%d failed)", successful_deletes, failed_deletes)
            total_processed += successful_deletes
            
            if failed_deletes > 0:
                logger.error("Failed deletes: %s", del_resp['Failed'])
                
        except Exception as e:
            logger.error("Failed to write/delete batch: %s", e)
            # Do NOT delete messages if S3 write fails - they will reappear after timeout
    
    logger.info("Drain run complete. Total processed: %d", total_processed)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "status": "complete",
            "processed_count": total_processed
        })
    }
