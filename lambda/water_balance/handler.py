"""
Lean-Ops: Water Balance Lambda

Checks that record counts are balanced across the pipeline:
    RAW count ≈ Semantic count + Errors count + DLQ count

Runs on schedule (EventBridge) and alerts on imbalance > 1%.

Usage:
    Triggered by EventBridge every 15 minutes per topic
"""

import json
import os
import boto3
from datetime import datetime, timezone, timedelta

athena = boto3.client('athena')
sqs = boto3.client('sqs')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

# Configuration
RAW_DATABASE = os.environ.get('RAW_DATABASE', 'iceberg_raw_db')
SEMANTIC_DATABASE = os.environ.get('SEMANTIC_DATABASE', 'semantic_db')
DLQ_URL = os.environ.get('DLQ_URL')
ALERTS_TOPIC_ARN = os.environ.get('ALERTS_TOPIC_ARN')
COUNTERS_TABLE = os.environ.get('COUNTERS_TABLE')
ATHENA_OUTPUT = os.environ.get('ATHENA_OUTPUT', 's3://lean-ops-bucket/athena-results/')
TOLERANCE = float(os.environ.get('TOLERANCE', '0.01'))  # 1%

counters = dynamodb.Table(COUNTERS_TABLE) if COUNTERS_TABLE else None


def query_athena(query: str) -> int:
    """Execute Athena query and return count."""
    try:
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        execution_id = response['QueryExecutionId']
        
        # Wait for completion
        while True:
            status = athena.get_query_execution(QueryExecutionId=execution_id)
            state = status['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                break
            elif state in ['FAILED', 'CANCELLED']:
                return 0
            
            import time
            time.sleep(1)
        
        # Get result
        result = athena.get_query_results(QueryExecutionId=execution_id)
        rows = result['ResultSet']['Rows']
        
        if len(rows) > 1:
            return int(rows[1]['Data'][0]['VarCharValue'])
        return 0
        
    except Exception as e:
        print(f"[ERROR] Athena query failed: {e}")
        return 0


def get_dlq_count(topic_name: str) -> int:
    """Get approximate count of messages for topic in DLQ."""
    if not DLQ_URL:
        return 0
    
    try:
        response = sqs.get_queue_attributes(
            QueueUrl=DLQ_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        # Note: This is total DLQ, not per-topic
        # For accurate per-topic, you'd need to query the messages
        return int(response['Attributes'].get('ApproximateNumberOfMessages', 0))
    except Exception as e:
        print(f"[ERROR] DLQ count failed: {e}")
        return 0


def send_alert(topic: str, message: str):
    """Send alert via SNS."""
    if ALERTS_TOPIC_ARN:
        sns.publish(
            TopicArn=ALERTS_TOPIC_ARN,
            Subject=f"Lean-Ops: Water Balance Alert - {topic}",
            Message=message
        )


def update_counters(topic: str, date: str, counts: dict):
    """Store metrics in DynamoDB."""
    if counters:
        counters.put_item(Item={
            'topic_name': topic,
            'date': date,
            'raw_count': counts['raw'],
            'semantic_count': counts['semantic'],
            'error_count': counts['errors'],
            'dlq_count': counts['dlq'],
            'balance_ok': counts['balance_ok'],
            'checked_at': datetime.now(timezone.utc).isoformat()
        })


def lambda_handler(event, context):
    """
    Check water balance for a topic.
    
    Event format:
    {
        "topic_name": "events",
        "date": "2026-01-07"  # optional, defaults to today
    }
    """
    topic_name = event.get('topic_name')
    if not topic_name:
        return {'error': 'topic_name required'}
    
    check_date = event.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    
    print(f"[INFO] Checking water balance for {topic_name} on {check_date}")
    
    # Query counts
    raw_count = query_athena(f"""
        SELECT COUNT(*) FROM "{RAW_DATABASE}"."{topic_name}_staging"
        WHERE date(from_unixtime(ingestion_ts)) = date('{check_date}')
    """)
    
    semantic_count = query_athena(f"""
        SELECT COUNT(*) FROM "{SEMANTIC_DATABASE}"."{topic_name}"
        WHERE date(last_updated_ts) = date('{check_date}')
    """)
    
    error_count = query_athena(f"""
        SELECT COUNT(*) FROM "{SEMANTIC_DATABASE}"."errors"
        WHERE topic_name = '{topic_name}' 
        AND date(error_ts) = date('{check_date}')
    """)
    
    dlq_count = get_dlq_count(topic_name)
    
    # Calculate balance
    expected = raw_count
    actual = semantic_count + error_count + dlq_count
    
    if expected == 0:
        balance_ok = True
        variance = 0.0
    else:
        variance = abs(expected - actual) / expected
        balance_ok = variance <= TOLERANCE
    
    counts = {
        'raw': raw_count,
        'semantic': semantic_count,
        'errors': error_count,
        'dlq': dlq_count,
        'expected': expected,
        'actual': actual,
        'variance': variance,
        'balance_ok': balance_ok
    }
    
    print(f"[INFO] Counts: {json.dumps(counts)}")
    
    # Update counters table
    update_counters(topic_name, check_date, counts)
    
    # Alert if imbalanced
    if not balance_ok:
        message = f"""
Water Balance Alert for {topic_name}

Date: {check_date}
RAW Count: {raw_count}
Semantic Count: {semantic_count}
Error Count: {error_count}
DLQ Count: {dlq_count}

Expected: {expected}
Actual: {actual}
Variance: {variance:.2%}

Please investigate missing records.
"""
        send_alert(topic_name, message)
        print(f"[WARNING] Water balance failed - variance {variance:.2%}")
    else:
        print(f"[INFO] Water balance OK - variance {variance:.2%}")
    
    return counts
