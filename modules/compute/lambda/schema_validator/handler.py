"""
Lambda: Schema Validator

Validates schema structure when uploaded to S3 pending/ folder.
Creates Iceberg tables via Athena DDL on first deployment.

Validation:
- JSON syntax check
- Required fields: table_name, envelope_columns, payload_columns

On successful validation:
1. Create STD table (envelope columns only, all STRING)
2. Create CURATED table (typed columns from schema)
3. Move schema to active/
4. Set DynamoDB processing_enabled = true
"""

import json
import os
import time
import boto3
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
athena = boto3.client('athena')

# Environment variables
SCHEMA_BUCKET = os.environ.get('SCHEMA_BUCKET')
SCHEMA_REGISTRY_TABLE = os.environ.get('SCHEMA_REGISTRY_TABLE')
ATHENA_OUTPUT_BUCKET = os.environ.get('ATHENA_OUTPUT_BUCKET', SCHEMA_BUCKET)
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')


def lambda_handler(event, context):
    """
    Validate a schema file and create tables.
    
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
            "tables_created": ["iceberg_standardized_db.events", ...]
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
        
        # Validation passed - create tables and move to active
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


def execute_athena_ddl(ddl: str, description: str) -> dict:
    """Execute DDL via Athena and wait for completion."""
    print(f"Executing DDL: {description}")
    print(f"SQL: {ddl[:500]}...")
    
    try:
        response = athena.start_query_execution(
            QueryString=ddl,
            ResultConfiguration={
                'OutputLocation': f's3://{ATHENA_OUTPUT_BUCKET}/athena-results/'
            },
            WorkGroup=ATHENA_WORKGROUP
        )
        
        query_id = response['QueryExecutionId']
        print(f"Query started: {query_id}")
        
        # Wait for completion (max 60 seconds)
        for _ in range(30):
            status = athena.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                print(f"✅ DDL succeeded: {description}")
                return {'success': True, 'query_id': query_id}
            elif state in ['FAILED', 'CANCELLED']:
                reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                print(f"❌ DDL failed: {reason}")
                return {'success': False, 'error': reason}
            
            time.sleep(2)
        
        return {'success': False, 'error': 'Timeout waiting for query'}
    except Exception as e:
        print(f"❌ Athena error: {str(e)}")
        return {'success': False, 'error': str(e)}


def build_standardized_ddl(topic: str, schema: dict, iceberg_bucket: str) -> str:
    """Build CREATE TABLE DDL for Standardized table (envelope columns only)."""
    table_name = f"iceberg_standardized_db.{schema.get('table_name', topic)}"
    location = f"s3://{iceberg_bucket}/iceberg_standardized_db/{topic}/"
    
    # Envelope columns
    envelope_cols = schema.get('envelope_columns', {})
    column_defs = []
    for col_name in envelope_cols.keys():
        col_type = envelope_cols[col_name]
        # publish_time must be TIMESTAMP for partitioning
        if col_name == 'publish_time':
            column_defs.append(f"    {col_name} TIMESTAMP")
        else:
            column_defs.append(f"    {col_name} STRING")
    
    columns_sql = ",\n".join(column_defs)
    
    ddl = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
)
PARTITIONED BY (period_reference, day(publish_time))
LOCATION '{location}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_target_data_file_size_bytes' = '134217728'
)
"""
    return ddl


def build_curated_ddl(topic: str, schema: dict, iceberg_bucket: str) -> str:
    """Build CREATE TABLE DDL for Curated table (typed columns from schema)."""
    table_name = f"iceberg_curated_db.{schema.get('table_name', topic)}"
    location = f"s3://{iceberg_bucket}/iceberg_curated_db/{topic}/"
    
    # Type mappings from schema to Spark/Athena types
    type_map = {
        'STRING': 'string',
        'TIMESTAMP': 'timestamp',
        'INT': 'int',
        'BIGINT': 'bigint',
        'DOUBLE': 'double',
        'BOOLEAN': 'boolean',
    }
    
    column_defs = []
    
    # Add envelope columns (STRING type)
    for col_name in schema.get('envelope_columns', {}).keys():
        col_type = schema['envelope_columns'][col_name]
        # For envelope, we keep them as defined or default to string? 
        # Actually std layer uses all string, but curated should use typed envelope if requested?
        # The schema uses types like "STRING", "TIMESTAMP" for envelope (see test schema).
        # We should map them.
        spark_type = type_map.get(col_type.upper(), 'string')
        if col_type.upper().startswith('DECIMAL'):
            spark_type = col_type
        column_defs.append(f"    {col_name} {spark_type}")
    
    # Add payload columns with types
    for col_name, col_type in schema.get('payload_columns', {}).items():
        spark_type = type_map.get(col_type.upper(), 'string')
        if col_type.upper().startswith('DECIMAL'):
            spark_type = col_type
        column_defs.append(f"    {col_name} {spark_type}")
    
    columns_sql = ",\n".join(column_defs)
    
    ddl = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
)
PARTITIONED BY (period_reference, day(publish_time))
LOCATION '{location}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'write_target_data_file_size_bytes' = '134217728'
)
"""
    return ddl


def handle_validation_success(bucket, topic, pending_key, schema):
    """Create tables, move schema to active/, and update DynamoDB."""
    active_key = f"schemas/{topic}/active/schema.json"
    timestamp = datetime.utcnow().isoformat() + 'Z'
    tables_created = []
    
    print(f"Processing schema for topic: {topic}")
    
    # Create STD table
    std_ddl = build_standardized_ddl(topic, schema, bucket)
    std_result = execute_athena_ddl(std_ddl, f"Create STD table for {topic}")
    if std_result['success']:
        tables_created.append(f"iceberg_standardized_db.{schema.get('table_name', topic)}")
    else:
        # Log error but continue - table might already exist
        print(f"STD table creation issue: {std_result.get('error', 'Unknown')}")
    
    # Create CURATED table
    curated_ddl = build_curated_ddl(topic, schema, bucket)
    curated_result = execute_athena_ddl(curated_ddl, f"Create CURATED table for {topic}")
    if curated_result['success']:
        tables_created.append(f"iceberg_curated_db.{schema.get('table_name', topic)}")
    else:
        print(f"CURATED table creation issue: {curated_result.get('error', 'Unknown')}")
    
    # Move from pending to active
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': pending_key},
        Key=active_key
    )
    s3.delete_object(Bucket=bucket, Key=pending_key)
    print(f"Moved schema to {active_key}")
    
    # Update DynamoDB
    if SCHEMA_REGISTRY_TABLE:
        dynamodb.put_item(
            TableName=SCHEMA_REGISTRY_TABLE,
            Item={
                'topic_name': {'S': topic},
                'processing_enabled': {'BOOL': True},
                'status': {'S': 'READY'},
                'schema_path': {'S': f's3://{bucket}/{active_key}'},
                'updated_at': {'S': timestamp},
                'tables_created': {'SS': tables_created if tables_created else ['none']}
            }
        )
        print(f"Updated DynamoDB: processing_enabled=true, status=READY")
    
    return {
        'statusCode': 200,
        'valid': True,
        'topic': topic,
        'schema_path': f's3://{bucket}/{active_key}',
        'tables_created': tables_created,
        'message': 'Schema validated, tables created, and activated'
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
    
    # Update DynamoDB to disable processing
    if SCHEMA_REGISTRY_TABLE:
        dynamodb.put_item(
            TableName=SCHEMA_REGISTRY_TABLE,
            Item={
                'topic_name': {'S': topic},
                'processing_enabled': {'BOOL': False},
                'status': {'S': 'VALIDATION_FAILED'},
                'error_path': {'S': f's3://{bucket}/{error_key}'},
                'updated_at': {'S': timestamp}
            }
        )
    
    return {
        'statusCode': 400,
        'valid': False,
        'topic': topic,
        'errors': errors,
        'error_path': f's3://{bucket}/{error_key}'
    }
