"""
Lean-Ops: Freshness SLA Lambda

Checks that topics are receiving data within their configured SLA window.
Alerts if no new data arrives within the expected timeframe.

Usage:
    Triggered by EventBridge every 15 minutes
"""

import json
import os
import boto3
from datetime import datetime, timezone, timedelta

athena = boto3.client('athena')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

# Configuration
RAW_DATABASE = os.environ.get('RAW_DATABASE', 'iceberg_raw_db')
TOPIC_REGISTRY_TABLE = os.environ.get('TOPIC_REGISTRY_TABLE')
ALERTS_TOPIC_ARN = os.environ.get('ALERTS_TOPIC_ARN')
ATHENA_OUTPUT = os.environ.get('ATHENA_OUTPUT', 's3://lean-ops-bucket/athena-results/')
DEFAULT_SLA_HOURS = int(os.environ.get('DEFAULT_SLA_HOURS', '4'))

topic_registry = dynamodb.Table(TOPIC_REGISTRY_TABLE) if TOPIC_REGISTRY_TABLE else None


def get_topic_config(topic_name: str) -> dict:
    """Get topic configuration from registry."""
    if topic_registry:
        try:
            response = topic_registry.get_item(Key={'topic_name': topic_name})
            return response.get('Item', {})
        except Exception as e:
            print(f"[ERROR] Failed to get topic config: {e}")
    
    return {'freshness_sla_hours': DEFAULT_SLA_HOURS}


def query_last_record_time(topic_name: str) -> datetime:
    """Get timestamp of most recent record in RAW table."""
    query = f"""
        SELECT MAX(from_unixtime(ingestion_ts)) as last_ts
        FROM "{RAW_DATABASE}"."{topic_name}_staging"
    """
    
    try:
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
        )
        execution_id = response['QueryExecutionId']
        
        # Wait for completion
        import time
        while True:
            status = athena.get_query_execution(QueryExecutionId=execution_id)
            state = status['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                break
            elif state in ['FAILED', 'CANCELLED']:
                return None
            
            time.sleep(1)
        
        # Get result
        result = athena.get_query_results(QueryExecutionId=execution_id)
        rows = result['ResultSet']['Rows']
        
        if len(rows) > 1 and rows[1]['Data'][0].get('VarCharValue'):
            ts_str = rows[1]['Data'][0]['VarCharValue']
            return datetime.fromisoformat(ts_str.replace(' ', 'T'))
        
        return None
        
    except Exception as e:
        print(f"[ERROR] Query failed: {e}")
        return None


def send_alert(topic: str, hours_since: float, sla_hours: int):
    """Send freshness SLA breach alert."""
    if ALERTS_TOPIC_ARN:
        message = f"""
Freshness SLA Breach for {topic}

No new data received in {hours_since:.1f} hours.
SLA threshold: {sla_hours} hours.

Please investigate data source.
"""
        sns.publish(
            TopicArn=ALERTS_TOPIC_ARN,
            Subject=f"Lean-Ops: Freshness SLA Breach - {topic}",
            Message=message
        )


def lambda_handler(event, context):
    """
    Check freshness SLA for a topic.
    
    Event format:
    {
        "topic_name": "events"
    }
    """
    topic_name = event.get('topic_name')
    if not topic_name:
        return {'error': 'topic_name required'}
    
    print(f"[INFO] Checking freshness for {topic_name}")
    
    # Get topic config
    config = get_topic_config(topic_name)
    sla_hours = config.get('freshness_sla_hours', DEFAULT_SLA_HOURS)
    
    # Get last record time
    last_record_ts = query_last_record_time(topic_name)
    
    if last_record_ts is None:
        print(f"[WARNING] No records found for {topic_name}")
        return {
            'topic_name': topic_name,
            'status': 'NO_DATA',
            'sla_hours': sla_hours
        }
    
    # Calculate hours since last record
    now = datetime.now(timezone.utc)
    # Ensure last_record_ts is timezone-aware
    if last_record_ts.tzinfo is None:
        last_record_ts = last_record_ts.replace(tzinfo=timezone.utc)
    
    hours_since = (now - last_record_ts).total_seconds() / 3600
    
    result = {
        'topic_name': topic_name,
        'last_record_ts': last_record_ts.isoformat(),
        'hours_since': round(hours_since, 2),
        'sla_hours': sla_hours,
        'sla_ok': hours_since <= sla_hours
    }
    
    print(f"[INFO] {json.dumps(result)}")
    
    # Alert if SLA breached
    if not result['sla_ok']:
        send_alert(topic_name, hours_since, sla_hours)
        print(f"[WARNING] Freshness SLA breached - {hours_since:.1f}h > {sla_hours}h")
    else:
        print(f"[INFO] Freshness OK - {hours_since:.1f}h <= {sla_hours}h")
    
    return result
