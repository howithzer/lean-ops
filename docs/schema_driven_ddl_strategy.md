# Schema-Driven DDL Strategy for Curated Layer

## Overview

Use an **OpenAPI/JSON Schema file** on S3 to drive the creation of the Curated table DDL. All payload columns are **STRING type** for maximum flexibility and easy schema evolution.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SCHEMA-DRIVEN DDL FLOW                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  S3: schemas/curated_schema.json                                            │
│     ↓                                                                       │
│  Lambda: ensure_curated_table                                               │
│     1. Read schema from S3                                                  │
│     2. Generate CREATE TABLE DDL (ALL STRING columns)                       │
│     3. Execute via Athena                                                   │
│     4. ALTER TABLE ADD PARTITION FIELD                                      │
│     5. ALTER TABLE SET TBLPROPERTIES                                        │
│     ↓                                                                       │
│  Glue Job: curated_processor                                                │
│     • Uses MERGE (not writeTo().createOrReplace())                          │
│     • Flattens json_payload → individual STRING columns (max depth 5)       │
│     • ALTER TABLE ADD COLUMNS for new fields (schema drift)                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Schema File Format

### Option A: JSON Schema (Recommended - Simpler)

```json
{
  "schema_version": "1.0.0",
  "table_name": "events",
  "description": "Curated events table - all STRING columns for flexibility",
  
  "envelope_columns": {
    "message_id":       { "description": "Network dedup key" },
    "idempotency_key":  { "description": "Business dedup key" },
    "topic_name":       { "description": "Source topic" },
    "period_reference": { "description": "Business period (YYYY-MM)", "partition": true },
    "correlation_id":   { "description": "Trace correlation" },
    "publish_time":     { "description": "Source timestamp", "partition": "day" },
    "ingestion_ts":     { "description": "RAW ingestion epoch", "type": "BIGINT" }
  },
  
  "payload_columns": {
    "session_id":       { "json_path": "$.sessionId" },
    "user_id":          { "json_path": "$.event.userId" },
    "event_type":       { "json_path": "$.event.eventType" },
    "application_id":   { "json_path": "$.event.applicationId" },
    "event_timestamp":  { "json_path": "$.event.timestamp" },
    "ip_address":       { "json_path": "$.event.metadata.ipAddress" },
    "user_agent":       { "json_path": "$.event.metadata.userAgent" },
    "device_id":        { "json_path": "$.event.metadata.deviceId" }
  },
  
  "table_properties": {
    "format": "parquet",
    "write_compression": "zstd",
    "write.target-file-size-bytes": "268435456",
    "commit.manifest.target-size-bytes": "8388608"
  },
  
  "partitioning": [
    { "field": "period_reference", "transform": "identity" },
    { "field": "publish_time", "transform": "day" }
  ]
}
```

### Option B: OpenAPI Spec (More Formal)

Use `components/schemas/CuratedEvent` definition as the source of truth.

---

## Lambda: ensure_curated_table

```python
import boto3
import json
import time

s3 = boto3.client('s3')
athena = boto3.client('athena')

def lambda_handler(event, context):
    """Ensure Curated table exists with proper DDL from schema."""
    
    database = event['database']
    table = event['table']
    schema_bucket = event['schema_bucket']
    schema_key = event['schema_key']
    iceberg_bucket = event['iceberg_bucket']
    
    # 1. Check if table already exists
    if table_exists(database, table):
        return {"status": "exists", "table": f"{database}.{table}"}
    
    # 2. Load schema from S3
    schema = load_schema(schema_bucket, schema_key)
    
    # 3. Generate CREATE TABLE DDL (ALL STRING except ingestion_ts)
    create_ddl = generate_create_table_ddl(database, table, schema, iceberg_bucket)
    execute_athena(create_ddl)
    
    # 4. Add partitioning via ALTER TABLE
    for partition in schema.get('partitioning', []):
        field = partition['field']
        transform = partition.get('transform', 'identity')
        
        if transform == 'identity':
            alter_sql = f"ALTER TABLE {database}.{table} ADD PARTITION FIELD {field}"
        else:
            alter_sql = f"ALTER TABLE {database}.{table} ADD PARTITION FIELD {transform}({field})"
        
        execute_athena(alter_sql)
    
    # 5. Set table properties
    props = schema.get('table_properties', {})
    if props:
        props_str = ", ".join([f"'{k}' = '{v}'" for k, v in props.items()])
        execute_athena(f"ALTER TABLE {database}.{table} SET TBLPROPERTIES ({props_str})")
    
    return {"status": "created", "table": f"{database}.{table}"}


def generate_create_table_ddl(database, table, schema, iceberg_bucket):
    """Generate CREATE TABLE with ALL STRING columns (except ingestion_ts)."""
    
    columns = []
    
    # Envelope columns
    for col_name, col_def in schema.get('envelope_columns', {}).items():
        col_type = col_def.get('type', 'STRING')  # Default to STRING
        description = col_def.get('description', '')
        columns.append(f"  {col_name} {col_type} COMMENT '{description}'")
    
    # Payload columns (ALWAYS STRING for flexibility)
    for col_name, col_def in schema.get('payload_columns', {}).items():
        description = col_def.get('description', col_def.get('json_path', ''))
        columns.append(f"  {col_name} STRING COMMENT '{description}'")
    
    columns_sql = ",\n".join(columns)
    
    ddl = f"""
CREATE TABLE IF NOT EXISTS {database}.{table} (
{columns_sql}
)
LOCATION 's3://{iceberg_bucket}/{database}/{table}/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
)
"""
    return ddl


def load_schema(bucket, key):
    """Load schema JSON from S3."""
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read().decode('utf-8'))


def table_exists(database, table):
    """Check if table exists in Glue Catalog."""
    glue = boto3.client('glue')
    try:
        glue.get_table(DatabaseName=database, Name=table)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


def execute_athena(sql, database='default'):
    """Execute SQL via Athena and wait for completion."""
    response = athena.start_query_execution(
        QueryString=sql,
        ResultConfiguration={'OutputLocation': f's3://{ATHENA_OUTPUT_BUCKET}/results/'}
    )
    query_id = response['QueryExecutionId']
    
    # Wait for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)
    
    if state != 'SUCCEEDED':
        raise Exception(f"Athena query failed: {sql}")
```

---

## Schema Drift Handling

When new fields appear in the payload:

```python
# In Glue job: curated_processor.py

def handle_schema_drift(df, curated_table):
    """Add new columns discovered in payload to Curated table."""
    
    curated_cols = set(spark.table(curated_table).columns)
    df_cols = set(df.columns)
    new_cols = df_cols - curated_cols
    
    for col in new_cols:
        # All drift columns are STRING for flexibility
        spark.sql(f"ALTER TABLE {curated_table} ADD COLUMNS ({col} STRING)")
        print(f"Added new column: {col} STRING")
```

---

## Workflow Integration

### Step Function: unified-orchestrator (with Schema Gate)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       UNIFIED ORCHESTRATOR                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. CheckSchemaExists (Lambda)                                              │
│     - Check S3: schemas/{topic_name}.json exists?                           │
│     ↓                                                                       │
│  2. Choice: Schema Found?                                                   │
│     ├─ NO  → LogSkipped → End (topic not ready for curation)                │
│     └─ YES ↓                                                                │
│  3. EnsureCuratedTable (Lambda)                                             │
│     - Read schema, generate DDL, apply if table doesn't exist               │
│     ↓                                                                       │
│  4. GetCheckpoint (Lambda)                                                  │
│     ↓                                                                       │
│  5. RunCurationJob (Glue)                                                   │
│     - MERGE with deduplication                                              │
│     - Handle schema drift (new columns)                                     │
│     ↓                                                                       │
│  6. UpdateCheckpoint (Lambda) → Success                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### ASL Definition (Snippet)

```json
{
  "StartAt": "CheckSchemaExists",
  "States": {
    "CheckSchemaExists": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:...:check_schema_exists",
      "Parameters": {
        "bucket": "lean-ops-development-iceberg",
        "key.$": "States.Format('schemas/{}.json', $.topic_name)"
      },
      "ResultPath": "$.schema_check",
      "Next": "SchemaGate"
    },
    
    "SchemaGate": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.schema_check.exists",
          "BooleanEquals": true,
          "Next": "EnsureCuratedTable"
        }
      ],
      "Default": "LogSkipped"
    },
    
    "LogSkipped": {
      "Type": "Pass",
      "Result": {
        "status": "skipped",
        "reason": "Schema file not found - topic not ready for curation"
      },
      "End": true
    },
    
    "EnsureCuratedTable": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:...:ensure_curated_table",
      "Parameters": {
        "database": "iceberg_curated_db",
        "table.$": "$.topic_name",
        "schema_bucket": "lean-ops-development-iceberg",
        "schema_key.$": "States.Format('schemas/{}.json', $.topic_name)"
      },
      "Next": "GetCheckpoint"
    }
  }
}
```

### Lambda: check_schema_exists

```python
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """Check if schema file exists for a topic."""
    bucket = event['bucket']
    key = event['key']
    
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return {
            "exists": True,
            "bucket": bucket,
            "key": key
        }
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return {
                "exists": False,
                "bucket": bucket,
                "key": key
            }
        raise
```

---

## Topic Onboarding Flow

```
NEW TOPIC ONBOARDING:

1. Data team defines schema: schemas/{topic_name}.json
   ↓
2. Upload to S3: s3://lean-ops-development-iceberg/schemas/orders.json
   ↓
3. Next Step Function execution auto-detects schema
   ↓
4. EnsureCuratedTable creates table with proper DDL
   ↓
5. Curation starts automatically!
```

### Benefits

| Benefit | Description |
|---------|-------------|
| **Self-service onboarding** | Teams upload schema file → curation starts |
| **No code changes** | New topics don't require Terraform/deploy |
| **Graceful skip** | Topics without schemas are silently skipped |
| **Schema as documentation** | JSON file documents expected fields |
| **Gate for production** | Topic only curated when schema approved |
```

---

## Benefits of This Approach

| Benefit | Description |
|---------|-------------|
| **Single Source of Truth** | Schema file defines table structure |
| **Easy Evolution** | ALL STRING columns → no type conflicts |
| **Type Safety Deferred** | Convert to proper types in Semantic layer |
| **Schema Versioning** | Schema file in S3 can be versioned (S3 versioning) |
| **Idempotent** | Lambda checks if table exists before creating |
| **Centralized Config** | One file for all column definitions |

---

## Migration Path

1. **Upload schema JSON to S3:** `s3://lean-ops-development-iceberg/schemas/curated_events_schema.json`
2. **Deploy Lambda:** `ensure_curated_table`
3. **Update Step Function:** Add `EnsureCuratedTable` as first step
4. **Modify Glue job:** Remove `writeTo().createOrReplace()`, always use MERGE
5. **Terraform:** Invoke Lambda after deploy to create initial table

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **ALL STRING columns** | Iceberg schema evolution works best with flexible types; type casting happens in Semantic layer |
| **JSON Schema over OpenAPI** | Simpler, easier to parse, sufficient for DDL generation |
| **Lambda over Terraform null_resource** | More robust, idempotent, can be triggered by Step Function |
| **Partitioning via ALTER TABLE** | Athena CREATE TABLE doesn't support composite partitioning |
| **Schema on S3** | Versioned, accessible to all components, easy to update |
