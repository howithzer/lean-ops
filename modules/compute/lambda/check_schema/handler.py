"""
Lambda: Check if Swagger/Schema file exists for Semantic trigger

Checks S3 for schema file presence. Used by orchestrator to conditionally
trigger Semantic pipeline after Curation.
"""

import json
import os
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

# Environment variables
SCHEMA_BUCKET = os.environ.get('SCHEMA_BUCKET')
SCHEMA_PREFIX = os.environ.get('SCHEMA_PREFIX', 'schemas/')


def lambda_handler(event, context):
    """
    Check if schema file exists for a given topic/table.
    
    Input:
        {
            "topic_name": "events" or
            "schema_key": "schemas/semantic_schema.json"
        }
    
    Output:
        {
            "schema_ready": true/false,
            "schema_path": "s3://bucket/key" (if exists)
        }
    """
    # Get topic name or direct schema key
    topic_name = event.get('topic_name')
    schema_key = event.get('schema_key')
    
    # Build schema key from topic name if not provided directly
    if not schema_key and topic_name:
        schema_key = f"{SCHEMA_PREFIX}{topic_name}/semantic_schema.json"
    
    if not schema_key:
        return {
            'statusCode': 400,
            'schema_ready': False,
            'error': 'Missing topic_name or schema_key'
        }
    
    try:
        # Check if schema file exists
        s3.head_object(Bucket=SCHEMA_BUCKET, Key=schema_key)
        
        return {
            'statusCode': 200,
            'schema_ready': True,
            'schema_path': f"s3://{SCHEMA_BUCKET}/{schema_key}"
        }
        
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return {
                'statusCode': 200,
                'schema_ready': False,
                'message': f'Schema not found: {schema_key}'
            }
        else:
            return {
                'statusCode': 500,
                'schema_ready': False,
                'error': str(e)
            }
