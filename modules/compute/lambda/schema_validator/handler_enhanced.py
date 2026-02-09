"""
Lambda: Enhanced Schema Validator with Backward Compatibility

Validates schema structure when uploaded to S3 pending/ folder.
Checks backward compatibility with active schema.
Creates Iceberg tables via Athena DDL on first deployment.
Triggers historical patching if schema is enhanced.

Enhancements over base handler:
- Backward compatibility validation
- Breaking change detection
- Schema versioning (archive old versions)
- Automatic backfill orchestration
- Drift logging

Validation:
- JSON syntax check
- Required fields: table_name, envelope_columns, payload_columns
- Backward compatibility (if active schema exists)
- Type compatibility checks
- CDE field validation

On successful validation:
1. Load active schema (if exists)
2. Validate backward compatibility
3. Archive old schema version (if enhancement)
4. Create/update tables via Athena DDL
5. Move new schema to active/
6. Update DynamoDB flag
7. Trigger backfill (if enhancement)
"""

import json
import os
import time
import boto3
from datetime import datetime
from typing import Dict, Optional

# Import compatibility validator
try:
    from compatibility import (
        validate_backward_compatibility,
        format_validation_report,
        get_backfill_config
    )
except ImportError:
    # For local testing
    import sys
    sys.path.append(os.path.dirname(__file__))
    from compatibility import (
        validate_backward_compatibility,
        format_validation_report,
        get_backfill_config
    )

s3 = boto3.client('s3')
dynamodb = boto3.client('dynamodb')
athena = boto3.client('athena')
stepfunctions = boto3.client('stepfunctions')
glue = boto3.client('glue')

# Environment variables
SCHEMA_BUCKET = os.environ.get('SCHEMA_BUCKET')
SCHEMA_REGISTRY_TABLE = os.environ.get('SCHEMA_REGISTRY_TABLE')
ATHENA_OUTPUT_BUCKET = os.environ.get('ATHENA_OUTPUT_BUCKET', SCHEMA_BUCKET)
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')
BACKFILL_STATE_MACHINE_ARN = os.environ.get('BACKFILL_STATE_MACHINE_ARN', '')
ICEBERG_BUCKET = os.environ.get('ICEBERG_BUCKET', SCHEMA_BUCKET)


def lambda_handler(event, context):
    """
    Validate schema file with backward compatibility checks.

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
            "is_enhancement": false,
            "requires_backfill": false,
            "tables_created": ["iceberg_standardized_db.events", ...],
            "backfill_job_id": "..." (if backfill triggered)
        }
    """
    # Extract S3 details
    bucket = event.get('detail', {}).get('bucket', {}).get('name') or SCHEMA_BUCKET
    key = event.get('detail', {}).get('object', {}).get('key', '')

    # For direct invocation (testing)
    if not key:
        bucket = event.get('bucket', SCHEMA_BUCKET)
        key = event.get('key', '')

    if not key or '/pending/' not in key:
        return {
            'statusCode': 400,
            'valid': False,
            'error': 'Invalid S3 key: must be in schemas/{topic}/pending/'
        }

    # Parse topic from key: schemas/{topic}/pending/schema.json
    topic = extract_topic_from_key(key)
    if not topic:
        return {
            'statusCode': 400,
            'valid': False,
            'error': f'Cannot extract topic from key: {key}'
        }

    print(f"Processing schema for topic: {topic}")

    try:
        # 1. Fetch new schema from S3
        new_schema = load_schema_from_s3(bucket, key)

        # 2. Validate basic structure
        structural_errors = validate_schema_structure(new_schema)
        if structural_errors:
            return handle_validation_failure(bucket, topic, structural_errors, key)

        # 3. Load active schema (if exists)
        active_schema = load_active_schema(bucket, topic)

        # 4. Validate backward compatibility (if active schema exists)
        compatibility_result = None
        if active_schema:
            print(f"Found active schema - validating backward compatibility")
            compatibility_result = validate_backward_compatibility(active_schema, new_schema)

            # Log validation report
            report = format_validation_report(compatibility_result)
            print(report)

            # Block if breaking changes detected
            if compatibility_result.has_breaking_changes:
                breaking_errors = [
                    {
                        'type': 'BREAKING_CHANGE',
                        'field': change.field_name,
                        'message': change.reason
                    }
                    for change in compatibility_result.breaking_changes
                ]
                return handle_validation_failure(
                    bucket, topic, breaking_errors, key,
                    compatibility_report=report
                )

        # 5. Archive old schema version (if this is an enhancement)
        if active_schema and compatibility_result and compatibility_result.is_enhancement:
            archive_schema_version(bucket, topic, active_schema)

        # 6. Create/update tables via Athena DDL
        tables_created = create_tables(topic, new_schema)

        # 7. Move schema to active/ and update DynamoDB
        move_to_active(bucket, topic, key, new_schema, tables_created)

        # 8. Update DynamoDB with schema status
        backfill_triggered = False
        backfill_job_id = None

        if compatibility_result and compatibility_result.requires_backfill:
            # Disable processing temporarily until backfill completes
            update_dynamodb_status(
                topic,
                processing_enabled=False,
                status='BACKFILL_PENDING',
                schema_path=f"s3://{bucket}/schemas/{topic}/active/schema.json",
                tables_created=tables_created
            )

            # Trigger backfill orchestration
            backfill_config = get_backfill_config(new_schema, compatibility_result)
            backfill_job_id = trigger_backfill(topic, new_schema, backfill_config)
            backfill_triggered = True

            print(f"✅ Schema validated (enhancement) - backfill job {backfill_job_id} started")
        else:
            # No backfill needed - enable processing immediately
            update_dynamodb_status(
                topic,
                processing_enabled=True,
                status='READY',
                schema_path=f"s3://{bucket}/schemas/{topic}/active/schema.json",
                tables_created=tables_created
            )

            print(f"✅ Schema validated - processing enabled")

        # 9. Log drift event (if enhancement)
        if compatibility_result and compatibility_result.is_enhancement:
            log_drift_event(topic, compatibility_result)

        return {
            'statusCode': 200,
            'valid': True,
            'topic': topic,
            'is_enhancement': compatibility_result.is_enhancement if compatibility_result else False,
            'requires_backfill': compatibility_result.requires_backfill if compatibility_result else False,
            'schema_path': f's3://{bucket}/schemas/{topic}/active/schema.json',
            'tables_created': tables_created,
            'backfill_triggered': backfill_triggered,
            'backfill_job_id': backfill_job_id,
            'message': 'Schema validated and activated'
        }

    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()

        return {
            'statusCode': 500,
            'valid': False,
            'topic': topic,
            'error': str(e)
        }


# =============================================================================
# SCHEMA LOADING
# =============================================================================

def load_schema_from_s3(bucket: str, key: str) -> Dict:
    """Load and parse schema JSON from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)


def load_active_schema(bucket: str, topic: str) -> Optional[Dict]:
    """Load currently active schema (returns None if doesn't exist)."""
    active_key = f"schemas/{topic}/active/schema.json"

    try:
        return load_schema_from_s3(bucket, active_key)
    except s3.exceptions.NoSuchKey:
        print(f"No active schema found for {topic} - this is a new topic")
        return None
    except Exception as e:
        print(f"Warning: Failed to load active schema: {e}")
        return None


# =============================================================================
# VALIDATION
# =============================================================================

def validate_schema_structure(schema: Dict) -> list:
    """
    Validate basic schema structure.

    Returns:
        List of error dicts (empty if valid)
    """
    errors = []

    # Required fields
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

    # Validate CDE fields exist in schema
    if 'cde_fields' in schema:
        all_columns = set()
        all_columns.update(schema.get('envelope_columns', {}).keys())
        all_columns.update(schema.get('payload_columns', {}).keys())

        cde_fields = schema['cde_fields']
        missing_cde = [f for f in cde_fields if f not in all_columns]

        if missing_cde:
            errors.append({
                'type': 'INVALID_CDE',
                'field': ','.join(missing_cde),
                'message': f'CDE fields not found in schema: {missing_cde}'
            })

    return errors


def extract_topic_from_key(key: str) -> Optional[str]:
    """Extract topic name from S3 key."""
    parts = key.split('/')
    if len(parts) >= 3 and parts[0] == 'schemas':
        return parts[1]
    return None


# =============================================================================
# SCHEMA VERSIONING
# =============================================================================

def archive_schema_version(bucket: str, topic: str, old_schema: Dict):
    """Archive old schema version with timestamp."""
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    version = old_schema.get('schema_version', 'unknown')

    archive_key = f"schemas/{topic}/archive/v{version}_{timestamp}.json"

    # Copy active schema to archive
    s3.copy_object(
        Bucket=bucket,
        CopySource={
            'Bucket': bucket,
            'Key': f"schemas/{topic}/active/schema.json"
        },
        Key=archive_key
    )

    print(f"Archived old schema to {archive_key}")


def move_to_active(bucket: str, topic: str, pending_key: str, schema: Dict, tables_created: list):
    """Move schema from pending/ to active/."""
    active_key = f"schemas/{topic}/active/schema.json"

    # Copy to active
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': pending_key},
        Key=active_key
    )

    # Delete from pending
    s3.delete_object(Bucket=bucket, Key=pending_key)

    print(f"Moved schema to {active_key}")


# =============================================================================
# TABLE CREATION
# =============================================================================

def create_tables(topic: str, schema: Dict) -> list:
    """Create Standardized and Curated Iceberg tables via Athena DDL."""
    tables_created = []

    # Create STD table
    std_ddl = build_standardized_ddl(topic, schema, ICEBERG_BUCKET)
    std_result = execute_athena_ddl(std_ddl, f"Create STD table for {topic}")

    if std_result['success']:
        tables_created.append(f"iceberg_standardized_db.{schema.get('table_name', topic)}")
    else:
        print(f"⚠️  STD table creation issue: {std_result.get('error', 'Unknown')}")

    # Create CURATED table
    curated_ddl = build_curated_ddl(topic, schema, ICEBERG_BUCKET)
    curated_result = execute_athena_ddl(curated_ddl, f"Create CURATED table for {topic}")

    if curated_result['success']:
        tables_created.append(f"iceberg_curated_db.{schema.get('table_name', topic)}")
    else:
        print(f"⚠️  CURATED table creation issue: {curated_result.get('error', 'Unknown')}")

    return tables_created


def execute_athena_ddl(ddl: str, description: str) -> dict:
    """Execute DDL via Athena and wait for completion."""
    print(f"Executing DDL: {description}")
    print(f"SQL preview: {ddl[:200]}...")

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
    """Build CREATE TABLE DDL for Standardized table (all STRING columns)."""
    table_name = f"iceberg_standardized_db.{schema.get('table_name', topic)}"
    location = f"s3://{iceberg_bucket}/iceberg_standardized_db/{topic}/"

    # Envelope columns
    envelope_cols = schema.get('envelope_columns', {})
    column_defs = []
    for col_name in envelope_cols.keys():
        # publish_time must be TIMESTAMP for partitioning
        if col_name == 'publish_time':
            column_defs.append(f"    {col_name} TIMESTAMP")
        else:
            column_defs.append(f"    {col_name} STRING")

    # Standard audit columns (all STD tables have these)
    column_defs.extend([
        "    json_payload STRING",
        "    ingestion_ts BIGINT",
        "    period_reference STRING"
    ])

    columns_sql = ",\n".join(column_defs)

    ddl = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
)
USING iceberg
PARTITIONED BY (period_reference, day(publish_time))
LOCATION '{location}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'format-version' = '2',
    'write.format.default' = 'parquet'
)
"""
    return ddl


def build_curated_ddl(topic: str, schema: dict, iceberg_bucket: str) -> str:
    """Build CREATE TABLE DDL for Curated table (typed columns)."""
    table_name = f"iceberg_curated_db.{schema.get('table_name', topic)}"
    location = f"s3://{iceberg_bucket}/iceberg_curated_db/{topic}/"

    # Type mappings
    type_map = {
        'STRING': 'string',
        'TIMESTAMP': 'timestamp',
        'INT': 'int',
        'BIGINT': 'bigint',
        'DOUBLE': 'double',
        'BOOLEAN': 'boolean',
    }

    column_defs = []

    # Envelope columns (with types from schema)
    for col_name, col_spec in schema.get('envelope_columns', {}).items():
        if isinstance(col_spec, dict):
            col_type = col_spec.get('type', 'STRING')
        else:
            col_type = str(col_spec)

        spark_type = type_map.get(col_type.upper(), 'string')
        if col_type.upper().startswith('DECIMAL'):
            spark_type = col_type
        column_defs.append(f"    {col_name} {spark_type}")

    # Payload columns (with types from schema)
    for col_name, col_spec in schema.get('payload_columns', {}).items():
        if isinstance(col_spec, dict):
            col_type = col_spec.get('type', 'STRING')
        else:
            col_type = str(col_spec)

        spark_type = type_map.get(col_type.upper(), 'string')
        if col_type.upper().startswith('DECIMAL'):
            spark_type = col_type
        column_defs.append(f"    {col_name} {spark_type}")

    # Audit columns
    column_defs.extend([
        "    first_seen_ts TIMESTAMP",
        "    last_updated_ts TIMESTAMP",
        "    _schema_version STRING",
        "    period_reference STRING"
    ])

    columns_sql = ",\n".join(column_defs)

    ddl = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
{columns_sql}
)
USING iceberg
PARTITIONED BY (period_reference, day(publish_time))
LOCATION '{location}'
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet',
    'format-version' = '2',
    'write.format.default' = 'parquet'
)
"""
    return ddl


# =============================================================================
# DYNAMODB UPDATES
# =============================================================================

def update_dynamodb_status(
    topic: str,
    processing_enabled: bool,
    status: str,
    schema_path: str,
    tables_created: list
):
    """Update schema_registry table with current status."""
    if not SCHEMA_REGISTRY_TABLE:
        print("Warning: SCHEMA_REGISTRY_TABLE not set, skipping DynamoDB update")
        return

    timestamp = datetime.utcnow().isoformat() + 'Z'

    dynamodb.put_item(
        TableName=SCHEMA_REGISTRY_TABLE,
        Item={
            'topic_name': {'S': topic},
            'processing_enabled': {'BOOL': processing_enabled},
            'status': {'S': status},
            'schema_path': {'S': schema_path},
            'updated_at': {'S': timestamp},
            'tables_created': {'SS': tables_created if tables_created else ['none']}
        }
    )

    print(f"Updated DynamoDB: processing_enabled={processing_enabled}, status={status}")


# =============================================================================
# BACKFILL ORCHESTRATION
# =============================================================================

def trigger_backfill(topic: str, schema: Dict, backfill_config: Dict) -> str:
    """
    Trigger backfill orchestrator for historical patching.

    Returns:
        Execution ARN of Step Function (or Glue job run ID if using Glue directly)
    """
    if not BACKFILL_STATE_MACHINE_ARN:
        print("⚠️  BACKFILL_STATE_MACHINE_ARN not set - skipping backfill trigger")
        return "SKIPPED"

    print(f"Triggering backfill for {topic} with config: {backfill_config}")

    # Prepare input for Step Function
    sfn_input = {
        'topic_name': topic,
        'schema_version': schema.get('schema_version', '2.0.0'),
        'backfill_config': backfill_config,
        'trigger_source': 'schema_validator'
    }

    try:
        response = stepfunctions.start_execution(
            stateMachineArn=BACKFILL_STATE_MACHINE_ARN,
            name=f"{topic}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            input=json.dumps(sfn_input)
        )

        execution_arn = response['executionArn']
        print(f"✅ Backfill Step Function started: {execution_arn}")
        return execution_arn

    except Exception as e:
        print(f"❌ Failed to trigger backfill: {str(e)}")
        return f"ERROR: {str(e)}"


# =============================================================================
# DRIFT LOGGING
# =============================================================================

def log_drift_event(topic: str, compatibility_result):
    """Log schema drift to drift_log table (for observability)."""
    # TODO: Implement drift_log table writes
    # For now, just log to CloudWatch
    print(f"Schema drift detected for {topic}:")
    for change in compatibility_result.changes:
        print(f"  - {change.change_type.value}: {change.field_name} ({change.reason})")


# =============================================================================
# ERROR HANDLING
# =============================================================================

def handle_validation_failure(
    bucket: str,
    topic: str,
    errors: list,
    pending_key: str,
    compatibility_report: str = None
):
    """Move schema to failed/ and write error.json."""
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
        print(f"Moved failed schema to {failed_key}")
    except Exception as e:
        print(f"Warning: Failed to move schema: {e}")

    # Write error.json
    error_doc = {
        'status': 'FAILED',
        'topic': topic,
        'timestamp': timestamp,
        'errors': errors,
        'compatibility_report': compatibility_report,
        'action_required': 'Fix breaking changes and re-upload to pending/'
    }

    s3.put_object(
        Bucket=bucket,
        Key=error_key,
        Body=json.dumps(error_doc, indent=2),
        ContentType='application/json'
    )

    print(f"Wrote error report to {error_key}")

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
        'error_path': f's3://{bucket}/{error_key}',
        'message': 'Schema validation failed - check error.json for details'
    }
