"""
Lambda: Schema Validator (Placeholder)

Validates schema structure when uploaded to S3 pending/ folder.
This is a placeholder for future EventBridge integration.

Current validation:
- JSON syntax check
- Required fields: table_name, envelope_columns, payload_columns

Future validation (Phase 3):
- Type safety checks (narrowing type changes)
- Column compatibility with existing Iceberg table
"""

import json
import os
import boto3
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')

# Environment variables
SCHEMA_BUCKET = os.environ.get('SCHEMA_BUCKET')
SCHEMA_REGISTRY_TABLE = os.environ.get('SCHEMA_REGISTRY_TABLE')


def lambda_handler(event, context):
    """
    Validate a schema file uploaded to S3.
    
    Input (from EventBridge S3 event):
        {
            "detail": {
                "bucket": {"name": "..."},
                "object": {"key": "schemas/events/pending/schema.json"}
            }
        }
    
    Output:
        {
            "valid": true/false,
            "topic": "events",
            "errors": [...] if invalid
        }
    """
    # Extract S3 details
    bucket = event.get('detail', {}).get('bucket', {}).get('name') or SCHEMA_BUCKET
    key = event.get('detail', {}).get('object', {}).get('key', '')
    
    # For direct invocation (testing)
    if not key:
        bucket = event.get('bucket', SCHEMA_BUCKET)
        key = event.get('key', '')
    
    if not key:
        return {
            'statusCode': 400,
            'valid': False,
            'error': 'Missing S3 key'
        }
    
    # Parse topic from key: schemas/{topic}/pending/schema.json
    parts = key.split('/')
    if len(parts) < 3:
        return {
            'statusCode': 400,
            'valid': False,
            'error': f'Invalid key format: {key}'
        }
    
    topic = parts[1]
    
    try:
        # Fetch schema from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        schema_content = response['Body'].read().decode('utf-8')
        
        # Parse JSON
        schema = json.loads(schema_content)
        
        # Validate required fields
        errors = []
        
        if 'table_name' not in schema:
            errors.append({
                'type': 'MISSING_FIELD',
                'field': 'table_name',
                'message': 'Schema must have a table_name'
            })
        
        if 'envelope_columns' not in schema:
            errors.append({
                'type': 'MISSING_FIELD',
                'field': 'envelope_columns',
                'message': 'Schema must have envelope_columns'
            })
        
        if 'payload_columns' not in schema:
            errors.append({
                'type': 'MISSING_FIELD',
                'field': 'payload_columns',
                'message': 'Schema must have payload_columns'
            })
        
        if errors:
            return handle_validation_failure(bucket, topic, errors)
        
        # Validation passed - move to active
        return handle_validation_success(bucket, topic, key, schema)
        
    except json.JSONDecodeError as e:
        return handle_validation_failure(bucket, topic, [{
            'type': 'JSON_SYNTAX',
            'message': f'Invalid JSON: {str(e)}'
        }])
    except Exception as e:
        return {
            'statusCode': 500,
            'valid': False,
            'error': str(e)
        }


def handle_validation_success(bucket, topic, pending_key, schema):
    """Move schema to active/ and update DynamoDB."""
    active_key = f"schemas/{topic}/active/schema.json"
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    # Move from pending to active
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': pending_key},
        Key=active_key
    )
    s3.delete_object(Bucket=bucket, Key=pending_key)
    
    # Update DynamoDB
    if SCHEMA_REGISTRY_TABLE:
        dynamodb.put_item(
            TableName=SCHEMA_REGISTRY_TABLE,
            Item={
                'topic_name': {'S': topic},
                'processing_enabled': {'BOOL': True},
                'status': {'S': 'READY'},
                'schema_path': {'S': f's3://{bucket}/{active_key}'},
                'updated_at': {'S': timestamp}
            }
        )
    
    return {
        'statusCode': 200,
        'valid': True,
        'topic': topic,
        'schema_path': f's3://{bucket}/{active_key}',
        'message': 'Schema validated and activated'
    }


def handle_validation_failure(bucket, topic, errors):
    """Move schema to failed/ and write error.json."""
    pending_key = f"schemas/{topic}/pending/schema.json"
    failed_key = f"schemas/{topic}/failed/schema.json"
    error_key = f"schemas/{topic}/failed/error.json"
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    # Move to failed
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': pending_key},
            Key=failed_key
        )
        s3.delete_object(Bucket=bucket, Key=pending_key)
    except:
        pass  # May not exist
    
    # Write error.json
    error_doc = {
        'status': 'FAILED',
        'topic': topic,
        'timestamp': timestamp,
        'errors': errors,
        'action_required': 'Fix schema and re-upload to pending/'
    }
    s3.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=json.dumps(error_doc, indent=2),
        ContentType='application/json'
    )
    
    # Update DynamoDB with error
    if SCHEMA_REGISTRY_TABLE:
        dynamodb.put_item(
            TableName=SCHEMA_REGISTRY_TABLE,
            Item={
                'topic_name': {'S': topic},
                'processing_enabled': {'BOOL': False},
                'status': {'S': 'ERROR'},
                'last_error': {'S': errors[0].get('message', 'Validation failed')},
                'updated_at': {'S': timestamp}
            }
        )
    
    return {
        'statusCode': 400,
        'valid': False,
        'topic': topic,
        'errors': errors,
        'error_file': f's3://{bucket}/{error_key}'
    }
