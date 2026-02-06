#!/bin/bash
# ==============================================================================
# FIX EVENTS_STAGING TABLE SCHEMA
# ==============================================================================
# This script drops and recreates the events_staging table with the correct schema.
# Run this ONCE to fix the "Column 'publish_time' does not exist" error.
# ==============================================================================

set -e

REGION="us-east-1"
DATABASE="iceberg_raw_db"
TABLE="events_staging"
BUCKET="lean-ops-development-iceberg"
WORKGROUP="primary"

echo "========================================"
echo "Fixing events_staging Table Schema"
echo "========================================"
echo ""

# Step 1: Drop the table
echo "Step 1: Dropping existing table (if exists)..."
QUERY_ID=$(aws athena start-query-execution \
  --region "$REGION" \
  --query-string "DROP TABLE IF EXISTS ${DATABASE}.${TABLE}" \
  --work-group "$WORKGROUP" \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query "QueryExecutionId" \
  --output text)

echo "Query ID: $QUERY_ID"

# Wait for drop to complete
for i in {1..12}; do
  STATUS=$(aws athena get-query-execution \
    --region "$REGION" \
    --query-execution-id "$QUERY_ID" \
    --query "QueryExecution.Status.State" \
    --output text)

  echo "Status: $STATUS"

  if [ "$STATUS" = "SUCCEEDED" ]; then
    echo "✅ Table dropped successfully!"
    break
  elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
    echo "⚠️  Drop failed (table may not exist) - continuing..."
    break
  fi

  sleep 2
done

echo ""
echo "Step 2: Recreating table with correct schema..."
echo ""

# Step 2: Create table with correct schema
DDL="CREATE TABLE IF NOT EXISTS ${DATABASE}.${TABLE} (
  topic_name        STRING,
  message_id        STRING,
  idempotency_key   STRING,
  period_reference  STRING,
  correlation_id    STRING,
  publish_time      TIMESTAMP,
  ingestion_ts      BIGINT,
  json_payload      STRING
)
PARTITIONED BY (day(publish_time))
LOCATION 's3://${BUCKET}/${DATABASE}/${TABLE}/'
TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet')"

QUERY_ID=$(aws athena start-query-execution \
  --region "$REGION" \
  --query-string "$DDL" \
  --work-group "$WORKGROUP" \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query "QueryExecutionId" \
  --output text)

echo "Query ID: $QUERY_ID"

# Wait for create to complete
for i in {1..12}; do
  STATUS=$(aws athena get-query-execution \
    --region "$REGION" \
    --query-execution-id "$QUERY_ID" \
    --query "QueryExecution.Status.State" \
    --output text)

  echo "Status: $STATUS"

  if [ "$STATUS" = "SUCCEEDED" ]; then
    echo "✅ Table created successfully with correct schema!"
    break
  elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
    ERROR=$(aws athena get-query-execution \
      --region "$REGION" \
      --query-execution-id "$QUERY_ID" \
      --query "QueryExecution.Status.StateChangeReason" \
      --output text)
    echo "❌ Create failed: $ERROR"
    exit 1
  fi

  sleep 2
done

echo ""
echo "Step 3: Verifying table schema..."
echo ""

# Step 3: Verify schema
QUERY_ID=$(aws athena start-query-execution \
  --region "$REGION" \
  --query-string "DESCRIBE ${DATABASE}.${TABLE}" \
  --work-group "$WORKGROUP" \
  --result-configuration "OutputLocation=s3://${BUCKET}/athena-results/" \
  --query "QueryExecutionId" \
  --output text)

# Wait for describe to complete
for i in {1..12}; do
  STATUS=$(aws athena get-query-execution \
    --region "$REGION" \
    --query-execution-id "$QUERY_ID" \
    --query "QueryExecution.Status.State" \
    --output text)

  if [ "$STATUS" = "SUCCEEDED" ]; then
    echo "✅ Verification complete!"
    echo ""
    echo "Table schema (check that publish_time is present):"
    aws athena get-query-results \
      --region "$REGION" \
      --query-execution-id "$QUERY_ID" \
      --query "ResultSet.Rows[*].Data[*].VarCharValue" \
      --output text | head -20
    break
  elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
    echo "⚠️  Verification failed"
    break
  fi

  sleep 2
done

echo ""
echo "========================================"
echo "✅ FIX COMPLETE!"
echo "========================================"
echo ""
echo "Next steps:"
echo "  1. Re-run your Step Function trigger"
echo "  2. The 'publish_time' column should now exist"
echo ""
