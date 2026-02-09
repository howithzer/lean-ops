#!/bin/bash
# ==============================================================================
# PAYMENTS E2E EMULATION TEST
# ==============================================================================
# Simulates full lifecycle for 'payments' topic:
# 1. Clean up old data/tables
# 2. Upload Schema (triggering validator)
# 3. Inject Data (via SQS -> Firehose)
# 4. Trigger Pipeline (Step Function)
# 5. Verify Results (Athena counts)
# ==============================================================================

set -euo pipefail

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DATA_INJECTOR_DIR="$PROJECT_ROOT/../Testing_Framework_EEH_2.0/sample-data-generator"
CONFIG_FILE="$SCRIPT_DIR/configs/payments_test.json"
SCHEMA_FILE="$PROJECT_ROOT/schemas/payments.json"

BUCKET="lean-ops-development-iceberg"
WORKGROUP="primary"
TOPIC="payments"

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Get State Machine ARN
get_state_machine_arn() {
    cd "$PROJECT_ROOT"
    STATE_MACHINE_ARN=$(terraform output -raw state_machine_arn 2>/dev/null || echo "")
    if [ -z "$STATE_MACHINE_ARN" ]; then
        STATE_MACHINE_ARN="arn:aws:states:us-east-1:487500748616:stateMachine:lean-ops-dev-unified-orchestrator"
    fi
}

# Run Athena Query
run_athena_query() {
    local query="$1"
    local query_id=$(aws athena start-query-execution \
        --query-string "$query" \
        --work-group "$WORKGROUP" \
        --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
        --query "QueryExecutionId" --output text 2>/dev/null)
    
    local status="RUNNING"
    while [ "$status" = "RUNNING" ] || [ "$status" = "QUEUED" ]; do
        sleep 1
        status=$(aws athena get-query-execution --query-execution-id "$query_id" --query "QueryExecution.Status.State" --output text 2>/dev/null || echo "FAILED")
    done
    
    if [ "$status" = "SUCCEEDED" ]; then
        aws athena get-query-results --query-execution-id "$query_id" --query "ResultSet.Rows[1].Data[0].VarCharValue" --output text 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Inject Data
inject_data() {
    log_info "Injecting data using $CONFIG_FILE..."
    cd "$DATA_INJECTOR_DIR"
    source .venv/bin/activate 2>/dev/null || true
    python -m data_injector.main --config "$CONFIG_FILE" 2>&1 | tail -5
    cd "$PROJECT_ROOT"
}

# Trigger Step Function
trigger_step_function() {
    get_state_machine_arn
    log_info "Triggering Step Function for topic: $TOPIC..."
    local execution_arn=$(aws stepfunctions start-execution \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --input "{\"topic_name\": \"$TOPIC\"}" \
        --query "executionArn" --output text)
    
    log_info "Execution ARN: $execution_arn"
    log_info "Waiting for completion..."
    
    local status="RUNNING"
    while [ "$status" = "RUNNING" ]; do
        sleep 5
        status=$(aws stepfunctions describe-execution --execution-arn "$execution_arn" --query "status" --output text)
    done
    
    if [ "$status" = "SUCCEEDED" ]; then
        log_info "Pipeline SUCCEEDED ✅"
    else
        log_error "Pipeline FAILED ($status) ❌"
        exit 1
    fi
}

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

# Phase 1: Clean
log_info "Cleaning up old data..."
aws s3 rm "s3://$BUCKET/schemas/$TOPIC/" --recursive
aws athena start-query-execution --query-string "DROP TABLE IF EXISTS iceberg_raw_db.${TOPIC}_staging" --work-group "$WORKGROUP" --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" >/dev/null
aws athena start-query-execution --query-string "DROP TABLE IF EXISTS iceberg_standardized_db.$TOPIC" --work-group "$WORKGROUP" --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" >/dev/null
aws athena start-query-execution --query-string "DROP TABLE IF EXISTS iceberg_curated_db.$TOPIC" --work-group "$WORKGROUP" --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" >/dev/null
sleep 5

# Phase 2: Upload Schema
log_info "Uploading schema..."
aws s3 cp "$SCHEMA_FILE" "s3://$BUCKET/schemas/$TOPIC/pending/schema.json"
sleep 15
# Verify schema activation
status=$(aws dynamodb get-item --table-name lean-ops-dev-schema-registry --key "{\"topic_name\": {\"S\": \"$TOPIC\"}}" --query "Item.status.S" --output text 2>/dev/null || echo "NOT_FOUND")
if [ "$status" != "READY" ]; then
    log_error "Schema validation failed! Status: $status"
    exit 1
fi
log_info "Schema Validated & Active via Lambda ✅"

# Phase 3: Inject Data
inject_data
log_info "Waiting 90s for Firehose to flush..."
sleep 90

# Phase 4: Run Pipeline
trigger_step_function

# Phase 5: Verify
log_info "Verifying results..."
raw_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_raw_db.${TOPIC}_staging")
std_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.$TOPIC")
cur_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_curated_db.$TOPIC")

log_info "RAW Count: $raw_count"
log_info "STD Count: $std_count"
log_info "CUR Count: $cur_count"

if [ "$cur_count" -gt 0 ]; then
    log_info "✅ SUCCESS: Payments E2E Test Passed!"
else
    log_error "❌ FAILURE: No records in Curated table."
    exit 1
fi
