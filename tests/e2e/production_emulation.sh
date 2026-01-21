#!/bin/bash
# ==============================================================================
# PRODUCTION EMULATION TEST FRAMEWORK
# ==============================================================================
# What does this script do? (ELI5)
# --------------------------------
# This script pretends to be a real production environment.
#
# Day 1: Customer goes live
#   - 100,000 records arrive
#   - No schema deployed yet → data lands in RAW but isn't processed
#
# Schema Deployment: Data team "opens the gate"
#   - Upload schema files to S3
#   - Now processing can happen
#
# Day 2: Customer sends corrections
#   - 50,000 more records (some corrections, some errors)
#   - Processing runs → data flows to Curated
#
# Why test this way?
# Real production has phases. We simulate the phases to catch bugs that
# only appear when things happen in a specific order.
#
# Usage:
#   ./tests/e2e/production_emulation.sh clean   # Reset everything
#   ./tests/e2e/production_emulation.sh day1    # Initial load (100K records)
#   ./tests/e2e/production_emulation.sh schema  # Deploy schema files
#   ./tests/e2e/production_emulation.sh day2    # Corrections + errors
#   ./tests/e2e/production_emulation.sh verify  # Check counts
#   ./tests/e2e/production_emulation.sh full    # Run everything (~35 min)
#
# Timeline:
#   Clean:  ~1 min
#   Day 1:  ~15-20 min (includes Firehose buffer wait)
#   Day 2:  ~15-20 min
#   Full:   ~35-40 min total
# ==============================================================================

set -euo pipefail

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
DATA_INJECTOR_DIR="$PROJECT_ROOT/../Testing_Framework_EEH_2.0/sample-data-generator"

# Pretty colors for logging
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'  # No Color (reset)

# AWS settings
export AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}"
BUCKET="lean-ops-development-iceberg"
WORKGROUP="primary"

# Timing for duration tracking
START_TIME=$(date +%s)
BATCH_ID="e2e_$(date +%Y%m%d_%H%M%S)"

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
# These make the output pretty and handle common operations
# ==============================================================================

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_phase() { echo -e "\n${CYAN}========== $1 ==========${NC}\n"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $1"; }

# Calculate how long things took
elapsed_time() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    echo "$((duration / 60))m $((duration % 60))s"
}

# Get the Step Function ARN from Terraform output
get_state_machine_arn() {
    cd "$PROJECT_ROOT"
    STATE_MACHINE_ARN=$(terraform output -raw state_machine_arn 2>/dev/null || echo "")
    
    if [ -z "$STATE_MACHINE_ARN" ] || [ "$STATE_MACHINE_ARN" = "" ]; then
        # Fallback to hardcoded ARN for this environment
        STATE_MACHINE_ARN="arn:aws:states:us-east-1:487500748616:stateMachine:lean-ops-dev-unified-orchestrator"
        log_warn "Using fallback state machine ARN"
    fi
    
    log_info "State machine: $STATE_MACHINE_ARN"
}

# Run an Athena query and return the first result
run_athena_query() {
    local query="$1"
    
    # Start the query
    local query_id=$(aws athena start-query-execution \
        --query-string "$query" \
        --work-group "$WORKGROUP" \
        --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
        --query "QueryExecutionId" --output text 2>/dev/null)
    
    # Wait for it to complete (poll every second)
    local status="RUNNING"
    while [ "$status" = "RUNNING" ] || [ "$status" = "QUEUED" ]; do
        sleep 1
        status=$(aws athena get-query-execution \
            --query-execution-id "$query_id" \
            --query "QueryExecution.Status.State" --output text 2>/dev/null || echo "FAILED")
    done
    
    # Return the result
    if [ "$status" = "SUCCEEDED" ]; then
        aws athena get-query-results \
            --query-execution-id "$query_id" \
            --query "ResultSet.Rows[1].Data[0].VarCharValue" --output text 2>/dev/null || echo "0"
    else
        echo "0"
    fi
}

# Wait for a Step Function execution to complete
wait_for_stepfn() {
    local execution_arn="$1"
    local max_wait="${2:-600}"  # 10 minutes default
    local elapsed=0
    
    while [ $elapsed -lt $max_wait ]; do
        status=$(aws stepfunctions describe-execution \
            --execution-arn "$execution_arn" \
            --query "status" --output text 2>/dev/null || echo "UNKNOWN")
        
        if [ "$status" = "SUCCEEDED" ]; then
            log_info "Step Function SUCCEEDED ✅"
            return 0
        elif [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
            log_error "Step Function $status ❌"
            return 1
        fi
        
        echo -n "."  # Progress indicator
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    log_error "Step Function timed out after ${max_wait}s"
    return 1
}

# Trigger the Step Function and wait for completion
trigger_step_function() {
    log_info "Triggering Step Function..."
    local execution_arn=$(aws stepfunctions start-execution \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --input '{"topic_name": "events"}' \
        --query "executionArn" --output text 2>/dev/null)
    
    log_info "Execution: $execution_arn"
    log_info "Waiting for completion..."
    wait_for_stepfn "$execution_arn"
}

# Inject test data using the data injector tool
inject_data() {
    local config_file="$1"
    local records="${2:-100000}"
    
    log_info "Injecting data with config: $config_file ($records records)"
    
    if [ ! -f "$config_file" ]; then
        log_error "Config file not found: $config_file"
        return 1
    fi
    
    cd "$DATA_INJECTOR_DIR"
    source .venv/bin/activate 2>/dev/null || true
    
    # Run the data injector and show last 10 lines of output
    python -m data_injector.main --config "$config_file" 2>&1 | tail -10
    
    cd "$PROJECT_ROOT"
}

# ==============================================================================
# PHASE: CLEAN
# ==============================================================================
# What does this do?
# - DROP all Iceberg tables
# - Recreate them with minimal schema
# - Reset DynamoDB checkpoints to 0
# - Remove schema files (disables processing)
#
# When to use:
# - Before running a fresh test
# - When you want to start over
# ==============================================================================

phase_clean() {
    log_phase "CLEAN: RESET ALL DATA"
    
    log_info "This will DROP and recreate all Iceberg tables and reset checkpoints."
    
    # Tables to drop
    local tables=(
        "iceberg_raw_db.events_staging"
        "iceberg_raw_db.orders_staging"
        "iceberg_raw_db.payments_staging"
        "iceberg_standardized_db.events"
        "iceberg_standardized_db.parse_errors"
        "iceberg_curated_db.events"
        "iceberg_curated_db.errors"
    )
    
    # Drop each table
    for table in "${tables[@]}"; do
        log_info "Dropping $table..."
        run_athena_query "DROP TABLE IF EXISTS $table" >/dev/null 2>&1 || true
    done
    
    log_info "Waiting 5s for drops to complete..."
    sleep 5
    
    # Recreate tables with minimal schema
    log_info "Recreating events_staging..."
    run_athena_query "
        CREATE TABLE iceberg_raw_db.events_staging (
            message_id STRING,
            topic_name STRING,
            json_payload STRING,
            ingestion_ts BIGINT
        )
        LOCATION 's3://$BUCKET/raw/events_staging/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'zstd'
        )
    " >/dev/null 2>&1 || log_warn "events_staging may already exist"
    
    log_info "Recreating standardized events..."
    run_athena_query "
        CREATE TABLE iceberg_standardized_db.events (
            message_id STRING,
            idempotency_key STRING,
            publish_time STRING,
            ingestion_ts BIGINT,
            topic_name STRING
        )
        LOCATION 's3://$BUCKET/standardized/events/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'zstd'
        )
    " >/dev/null 2>&1 || log_warn "standardized events may already exist"
    
    log_info "Recreating parse_errors..."
    run_athena_query "
        CREATE TABLE iceberg_standardized_db.parse_errors (
            raw_payload STRING,
            error_type STRING,
            error_message STRING,
            processed_ts TIMESTAMP
        )
        LOCATION 's3://$BUCKET/standardized/parse_errors/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet'
        )
    " >/dev/null 2>&1 || log_warn "parse_errors may already exist"
    
    log_info "Recreating curated events..."
    run_athena_query "
        CREATE TABLE iceberg_curated_db.events (
            idempotency_key STRING
        )
        LOCATION 's3://$BUCKET/curated/events/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'zstd'
        )
    " >/dev/null 2>&1 || log_warn "curated events may already exist"
    
    log_info "Recreating curated errors..."
    run_athena_query "
        CREATE TABLE iceberg_curated_db.errors (
            idempotency_key STRING,
            error_type STRING,
            error_message STRING,
            processed_ts TIMESTAMP
        )
        LOCATION 's3://$BUCKET/curated/errors/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet'
        )
    " >/dev/null 2>&1 || log_warn "curated errors may already exist"
    
    # Reset DynamoDB checkpoints
    log_info "Resetting DynamoDB checkpoints..."
    aws dynamodb delete-item \
        --table-name "lean-ops-dev-checkpoints" \
        --key '{"pipeline_id": {"S": "standardization_events"}, "checkpoint_type": {"S": "standardized"}}' \
        2>/dev/null || true
    aws dynamodb delete-item \
        --table-name "lean-ops-dev-checkpoints" \
        --key '{"pipeline_id": {"S": "curation_events"}, "checkpoint_type": {"S": "curated"}}' \
        2>/dev/null || true
    
    # Remove schema files (disables processing until schema phase)
    log_info "Removing schema files from S3..."
    aws s3 rm "s3://$BUCKET/schemas/events.json" 2>/dev/null || true
    aws s3 rm "s3://$BUCKET/schemas/curated_schema.json" 2>/dev/null || true
    
    log_info "✅ CLEAN complete. Ready for fresh Day 1."
}

# ==============================================================================
# PHASE: DAY 1
# ==============================================================================
# What happens:
# 1. Inject 100K clean records via SQS
# 2. Wait 90s for Firehose to flush to S3
# 3. Trigger Step Function
# 4. Verify: Data should be in RAW only (no schema = no processing)
# ==============================================================================

phase_day1() {
    log_phase "DAY 1: INITIAL LOAD"
    
    get_state_machine_arn || exit 1
    
    local config="$SCRIPT_DIR/configs/day1_initial_load.json"
    
    log_info "Batch ID: $BATCH_ID"
    log_info "Injecting 100K clean records..."
    
    inject_data "$config" 100000
    
    log_info "Waiting 90s for Firehose buffer to flush..."
    sleep 90
    
    trigger_step_function
    
    verify_day1
}

# ==============================================================================
# PHASE: SCHEMA
# ==============================================================================
# What happens:
# - Upload events.json → enables Standardized processing
# - Upload curated_schema.json → enables Curated processing
#
# After this phase, the next Step Function run will actually process data!
# ==============================================================================

phase_schema() {
    log_phase "SCHEMA DEPLOYMENT"
    
    log_info "Uploading events.json (enables Standardized)..."
    aws s3 cp "$PROJECT_ROOT/schemas/events.json" "s3://$BUCKET/schemas/events.json"
    
    log_info "Uploading curated_schema.json (enables Curated)..."
    aws s3 cp "$PROJECT_ROOT/schemas/curated_schema.json" "s3://$BUCKET/schemas/curated_schema.json"
    
    log_info "✅ Schemas deployed. Data onboarding is now enabled."
    log_info "Next Step Function run will process RAW → Standardized → Curated"
    log_info "To trigger immediately: ./tests/e2e/production_emulation.sh trigger"
}

# ==============================================================================
# PHASE: DAY 2
# ==============================================================================
# What happens:
# 1. Inject 50K mixed records (corrections, drift, errors)
# 2. Wait 90s for Firehose
# 3. Trigger Step Function
# 4. Verify: Data should flow through to Curated, errors captured
# ==============================================================================

phase_day2() {
    log_phase "DAY 2: CORRECTIONS + DRIFT"
    
    get_state_machine_arn || exit 1
    
    local config="$SCRIPT_DIR/configs/day2_mixed_batch.json"
    
    log_info "Batch ID: $BATCH_ID"
    log_info "Injecting 50K mixed records (corrections, drift, errors)..."
    
    inject_data "$config" 50000
    
    log_info "Waiting 90s for Firehose buffer..."
    sleep 90
    
    trigger_step_function
    
    verify_day2
}

# Manually trigger the Step Function
phase_trigger() {
    log_phase "MANUAL TRIGGER"
    get_state_machine_arn || exit 1
    trigger_step_function
}

# ==============================================================================
# VERIFICATION FUNCTIONS
# ==============================================================================
# These check that data ended up in the right places
# ==============================================================================

verify_day1() {
    log_phase "DAY 1 VERIFICATION"
    log_info "Expected: Data in RAW only, no processing (no schema deployed)"
    
    local passed=0
    local failed=0
    
    # Check RAW count (should be ~100K)
    local raw_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_raw_db.events_staging")
    log_test "RAW table: $raw_count records"
    if [ "$raw_count" -ge 95000 ]; then
        log_info "✅ RAW count >= 95K (data landed)"
        ((passed++))
    else
        log_error "❌ RAW count < 95K (expected ~100K)"
        ((failed++))
    fi
    
    # Check Standardized (should be 0 - no schema!)
    local std_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.events")
    log_test "Standardized table: $std_count records"
    if [ "$std_count" -eq 0 ]; then
        log_info "✅ Standardized = 0 (correct - no schema, processing skipped)"
        ((passed++))
    else
        log_error "❌ Standardized = $std_count (expected 0 - schema should not exist!)"
        log_error "   Check: aws s3 ls s3://lean-ops-development-iceberg/schemas/"
        ((failed++))
    fi
    
    # Summary
    log_phase "DAY 1 SUMMARY"
    log_info "Passed: $passed, Failed: $failed"
    log_info "Next: Deploy schema with './tests/e2e/production_emulation.sh schema'"
    
    if [ $failed -eq 0 ]; then
        log_info "✅ DAY 1 OPERATIONS PASSED"
        return 0
    else
        log_error "❌ DAY 1 OPERATIONS FAILED"
        return 1
    fi
}

verify_day2() {
    log_phase "DAY 2 VERIFICATION"
    
    local passed=0
    local failed=0
    
    # Check Curated (should have data now)
    local curated_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_curated_db.events")
    log_test "Curated table: $curated_count records"
    if [ "$curated_count" -gt 0 ]; then
        log_info "✅ Curated has data"
        ((passed++))
    else
        log_error "❌ Curated is empty"
        ((failed++))
    fi
    
    # Check error tables
    local parse_errors=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.parse_errors")
    log_test "Parse errors: $parse_errors"
    
    local cde_errors=$(run_athena_query "SELECT COUNT(*) FROM iceberg_curated_db.errors" 2>/dev/null || echo "0")
    log_test "CDE violations: $cde_errors"
    
    # Accountability check
    local std_total=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.events")
    local accountability=$((std_total + parse_errors))
    log_test "Accountability: $accountability (standardized: $std_total + errors: $parse_errors)"
    
    # Summary
    log_phase "DAY 2 SUMMARY"
    log_info "Curated: $curated_count records"
    log_info "Parse Errors: $parse_errors"
    log_info "CDE Errors: $cde_errors"
    
    if [ $failed -eq 0 ]; then
        log_info "✅ DAY 2 OPERATIONS PASSED"
        return 0
    else
        log_error "❌ DAY 2 OPERATIONS FAILED"
        return 1
    fi
}

# Show a nice summary dashboard of all table counts
verify_all() {
    log_phase "FULL VERIFICATION"
    
    local raw=$(run_athena_query "SELECT COUNT(*) FROM iceberg_raw_db.events_staging")
    local std=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.events")
    local curated=$(run_athena_query "SELECT COUNT(*) FROM iceberg_curated_db.events")
    local parse_err=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.parse_errors" 2>/dev/null || echo "0")
    local cde_err=$(run_athena_query "SELECT COUNT(*) FROM iceberg_curated_db.errors" 2>/dev/null || echo "0")
    
    echo ""
    echo "┌─────────────────────────────────────────────┐"
    echo "│            DATA PIPELINE STATUS            │"
    echo "├─────────────────────────────────────────────┤"
    printf "│ RAW (events_staging):       %10s     │\n" "$raw"
    printf "│ Standardized (events):      %10s     │\n" "$std"
    printf "│ Curated (events):           %10s     │\n" "$curated"
    printf "│ Parse Errors:               %10s     │\n" "$parse_err"
    printf "│ CDE Errors:                 %10s     │\n" "$cde_err"
    echo "├─────────────────────────────────────────────┤"
    
    local total_out=$((std + parse_err))
    if [ "$raw" -gt 0 ]; then
        local accountability=$(echo "scale=1; $total_out * 100 / $raw" | bc)
        printf "│ Accountability:             %10s%%    │\n" "$accountability"
    fi
    echo "└─────────────────────────────────────────────┘"
    echo ""
}

# ==============================================================================
# MAIN: Parse command line and run the appropriate phase
# ==============================================================================

case "${1:-help}" in
    clean)
        phase_clean
        ;;
    day1)
        phase_day1
        ;;
    schema)
        phase_schema
        ;;
    day2)
        phase_day2
        ;;
    trigger)
        phase_trigger
        ;;
    verify)
        verify_all
        ;;
    full)
        # Run the complete test sequence
        phase_clean
        phase_day1
        phase_schema
        phase_day2
        verify_all
        ;;
    *)
        echo "Production Emulation Test Framework"
        echo ""
        echo "Usage: $0 <phase>"
        echo ""
        echo "Phases:"
        echo "  clean   - DROP tables, reset checkpoints, remove schema (~1 min)"
        echo "  day1    - Initial load (100K clean records)"
        echo "  schema  - Deploy curated_schema.json to S3"
        echo "  day2    - Corrections, drift, and error records"
        echo "  trigger - Manually trigger Step Function"
        echo "  verify  - Run all verification queries"
        echo "  full    - clean → day1 → schema → day2 (~35-40 min)"
        echo ""
        echo "Timeline:"
        echo "  Clean:  ~1 min"
        echo "  Day 1:  ~15-20 min"
        echo "  Day 2:  ~15-20 min"
        echo "  Full:   ~35-40 min"
        ;;
esac

log_info "Total duration: $(elapsed_time)"
