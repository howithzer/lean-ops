"""
Circuit Breaker Lambda: Disable/Enable SQS Event Source Mappings

This Lambda is triggered by CloudWatch Alarms to:
1. DISABLE ESMs when error rate exceeds threshold (ALARM state)
2. RE-ENABLE ESMs when errors subside (OK state)

Triggered by:
- CloudWatch Alarm → SNS → This Lambda
"""

import json
import os
import boto3

lambda_client = boto3.client('lambda')

SQS_PROCESSOR_FUNCTION = os.environ.get('SQS_PROCESSOR_FUNCTION')


def get_event_source_mappings(function_name):
    """Get all event source mappings for the SQS processor function."""
    mappings = []
    paginator = lambda_client.get_paginator('list_event_source_mappings')
    
    for page in paginator.paginate(FunctionName=function_name):
        for mapping in page.get('EventSourceMappings', []):
            # Only include SQS triggers (not DLQ)
            if 'sqs' in mapping.get('EventSourceArn', '').lower():
                if 'dlq' not in mapping.get('EventSourceArn', '').lower():
                    mappings.append(mapping)
    
    return mappings


def set_esm_state(mapping_uuid, enabled):
    """Enable or disable an event source mapping."""
    lambda_client.update_event_source_mapping(
        UUID=mapping_uuid,
        Enabled=enabled
    )


def lambda_handler(event, context):
    """
    Handle CloudWatch Alarm notifications.
    
    Event format (from SNS):
    {
        "Records": [{
            "Sns": {
                "Message": "{\"AlarmName\":\"...\",\"NewStateValue\":\"ALARM\"}"
            }
        }]
    }
    """
    
    for record in event.get('Records', []):
        sns_message = record.get('Sns', {}).get('Message', '{}')
        
        try:
            alarm_data = json.loads(sns_message)
        except json.JSONDecodeError:
            print(f"Could not parse SNS message: {sns_message}")
            continue
        
        alarm_name = alarm_data.get('AlarmName', 'unknown')
        new_state = alarm_data.get('NewStateValue', '')
        
        print(f"Circuit breaker triggered: alarm={alarm_name}, state={new_state}")
        
        # Get all ESMs for the SQS processor
        mappings = get_event_source_mappings(SQS_PROCESSOR_FUNCTION)
        
        if new_state == 'ALARM':
            # OPEN CIRCUIT - Disable all ESMs
            print(f"OPENING CIRCUIT: Disabling {len(mappings)} event source mappings")
            for m in mappings:
                try:
                    set_esm_state(m['UUID'], enabled=False)
                    print(f"  Disabled: {m['EventSourceArn']}")
                except Exception as e:
                    print(f"  Error disabling {m['UUID']}: {e}")
        
        elif new_state == 'OK':
            # CLOSE CIRCUIT - Re-enable ESMs
            print(f"CLOSING CIRCUIT: Re-enabling {len(mappings)} event source mappings")
            for m in mappings:
                try:
                    set_esm_state(m['UUID'], enabled=True)
                    print(f"  Enabled: {m['EventSourceArn']}")
                except Exception as e:
                    print(f"  Error enabling {m['UUID']}: {e}")
        
        else:
            print(f"Ignoring state: {new_state}")
    
    return {
        'statusCode': 200,
        'body': 'Circuit breaker processed'
    }
