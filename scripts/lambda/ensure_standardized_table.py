"""
Lambda: ensure_standardized_table
============================
Ensures a Curated table exists with proper DDL, partitioning, and properties.
Reads schema from S3 and generates CREATE TABLE + ALTER TABLE statements.
ALL payload columns are STRING type for maximum flexibility.
"""

import boto3
import json
import time
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
athena = boto3.client('athena')
glue = boto3.client('glue')

# Configuration from environment
ATHENA_OUTPUT_BUCKET = os.environ.get('ATHENA_OUTPUT_BUCKET', 'lean-ops-development-iceberg')
ATHENA_WORKGROUP = os.environ.get('ATHENA_WORKGROUP', 'primary')


def lambda_handler(event, context):
    """
    Ensure Curated table exists with proper DDL from schema.
    
    Input:
        {
            "database": "iceberg_standardized_db",
            "table": "events",
            "schema_bucket": "lean-ops-development-iceberg",
            "schema_key": "schemas/events.json",
            "iceberg_bucket": "lean-ops-development-iceberg"
        }
    
    Output:
        {
            "status": "created" | "exists",
            "table": "iceberg_standardized_db.events",
            "columns_count": 16
        }
    """
    database = event['database']
    table = event['table']
    schema_bucket = event['schema_bucket']
    schema_key = event['schema_key']
    iceberg_bucket = event.get('iceberg_bucket', schema_bucket)
    
    full_table = f"{database}.{table}"
    logger.info(f"Ensuring Curated table exists: {full_table}")
    
    # 1. Check if table already exists
    if table_exists(database, table):
        logger.info(f"Table already exists: {full_table}")
        return {
            "status": "exists",
            "table": full_table,
            "columns_count": 0  # Existing table - column count not computed
        }
    
    # 2. Load schema from S3
    logger.info(f"Loading schema from s3://{schema_bucket}/{schema_key}")
    schema = load_schema(schema_bucket, schema_key)
    
    # 3. Generate CREATE TABLE DDL
    create_ddl = generate_create_table_ddl(database, table, schema, iceberg_bucket)
    logger.info(f"Generated DDL:\n{create_ddl}")
    
    execute_athena_query(create_ddl)
    logger.info(f"Table created: {full_table}")
    
    # 4. Add partitioning via ALTER TABLE
    for partition in schema.get('partitioning', []):
        field = partition['field']
        transform = partition.get('transform', 'identity')
        
        if transform == 'identity':
            alter_sql = f"ALTER TABLE {database}.{table} ADD PARTITION FIELD {field}"
        else:
            alter_sql = f"ALTER TABLE {database}.{table} ADD PARTITION FIELD {transform}({field})"
        
        logger.info(f"Adding partition: {alter_sql}")
        execute_athena_query(alter_sql)
    
    # 5. Set table properties
    props = schema.get('table_properties', {})
    if props:
        # Note: Athena has limited SET TBLPROPERTIES support for Iceberg
        # Some properties are set at CREATE TABLE time only
        logger.info(f"Table properties configured: {props}")
    
    # Count columns
    envelope_cols = len(schema.get('envelope_columns', {}))
    payload_cols = len(schema.get('payload_columns', {}))
    
    return {
        "status": "created",
        "table": full_table,
        "columns_count": envelope_cols + payload_cols
    }


def table_exists(database: str, table: str) -> bool:
    """Check if table exists in Glue Catalog."""
    try:
        glue.get_table(DatabaseName=database, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def load_schema(bucket: str, key: str) -> dict:
    """Load schema JSON from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))


def generate_create_table_ddl(database: str, table: str, schema: dict, iceberg_bucket: str) -> str:
    """
    Generate CREATE TABLE with proper column types.
    - Envelope columns: use specified type or STRING
    - Payload columns: ALWAYS STRING for flexibility
    """
    columns = []
    
    # Envelope columns
    for col_name, col_def in schema.get('envelope_columns', {}).items():
        col_type = col_def.get('type', 'STRING')
        description = col_def.get('description', '').replace("'", "''")
        columns.append(f"  {col_name} {col_type} COMMENT '{description}'")
    
    # Payload columns (ALWAYS STRING for maximum flexibility)
    for col_name, col_def in schema.get('payload_columns', {}).items():
        description = col_def.get('description', '').replace("'", "''")
        columns.append(f"  {col_name} STRING COMMENT '{description}'")
    
    columns_sql = ",\n".join(columns)
    
    # Get table properties
    props = schema.get('table_properties', {})
    
    ddl = f"""
CREATE TABLE IF NOT EXISTS {database}.{table} (
{columns_sql}
)
LOCATION 's3://{iceberg_bucket}/{database}/{table}/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = '{props.get('format', 'parquet')}',
  'write_compression' = '{props.get('write_compression', 'zstd')}'
)
"""
    return ddl.strip()


def execute_athena_query(sql: str, max_wait_seconds: int = 120) -> dict:
    """Execute SQL via Athena and wait for completion."""
    logger.info(f"Executing Athena query: {sql[:100]}...")
    
    response = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={
            'OutputLocation': f's3://{ATHENA_OUTPUT_BUCKET}/athena-results/'
        }
    )
    query_id = response['QueryExecutionId']
    logger.info(f"Started Athena query: {query_id}")
    
    # Poll for completion
    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            logger.info(f"Query succeeded: {query_id}")
            return result
        elif state in ['FAILED', 'CANCELLED']:
            reason = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Athena query {state}: {reason}\nSQL: {sql}")
        
        time.sleep(2)
    
    raise Exception(f"Athena query timed out after {max_wait_seconds}s")
