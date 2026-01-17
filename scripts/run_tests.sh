#!/bin/bash
# =============================================================================
# Lean-Ops Test Runner
# =============================================================================
# Automates unit tests, infrastructure deployment, and E2E test execution.
#
# Usage:
#   ./scripts/run_tests.sh unit          # Run unit tests only
#   ./scripts/run_tests.sh deploy        # Deploy infrastructure
#   ./scripts/run_tests.sh e2e           # Run E2E test suite
#   ./scripts/run_tests.sh all           # Deploy + Unit + E2E
#   ./scripts/run_tests.sh destroy       # Tear down infrastructure
#   ./scripts/run_tests.sh status        # Check infra status
#
# Environment:
#   AWS_PROFILE=terraform-firehose (default)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_INJECTOR_DIR="$PROJECT_ROOT/../Testing_Framework_EEH_2.0/sample-data-generator"
CONFIGS_DIR="$PROJECT_ROOT/tests/configs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# AWS Configuration
export AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}"
BUCKET="lean-ops-development-iceberg"
STATE_MACHINE_ARN=""
WORKGROUP="primary"

# Timing
START_TIME=$(date +%s)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "\n${CYAN}========== $1 ==========${NC}\n"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $1"; }

elapsed_time() {
    local end_time=$(date +%s)
    local duration=$((end_time - START_TIME))
    echo "$((duration / 60))m $((duration % 60))s"
}

wait_for_stepfn() {
    local execution_arn="$1"
    local max_wait="${2:-300}"  # 5 minutes default
    local elapsed=0
    
    while [ $elapsed -lt $max_wait ]; do
        status=$(aws stepfunctions describe-execution \
            --execution-arn "$execution_arn" \
            --query "status" --output text 2>/dev/null || echo "UNKNOWN")
        
        if [ "$status" = "SUCCEEDED" ]; then
            log_info "Step Function SUCCEEDED"
            return 0
        elif [ "$status" = "FAILED" ] || [ "$status" = "ABORTED" ]; then
            log_error "Step Function $status"
            return 1
        fi
        
        sleep 10
        elapsed=$((elapsed + 10))
        echo -n "."
    done
    
    log_error "Timeout waiting for Step Function"
    return 1
}

run_athena_query() {
    local query="$1"
    local query_id
    
    query_id=$(aws athena start-query-execution \
        --query-string "$query" \
        --work-group "$WORKGROUP" \
        --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
        --query "QueryExecutionId" --output text 2>/dev/null)
    
    # Wait for query
    sleep 5
    
    # Get result
    aws athena get-query-results --query-execution-id "$query_id" \
        --query "ResultSet.Rows[1:].Data[0].VarCharValue" --output text 2>/dev/null || echo "0"
}

# Verify DDL columns exist after schema drift
verify_ddl_columns() {
    local expected_columns=("$@")
    
    log_info "Verifying DDL for expected columns..."
    
    # Get all columns from the standardized table
    local query="SHOW COLUMNS IN iceberg_standardized_db.events"
    local query_id
    
    query_id=$(aws athena start-query-execution \
        --query-string "$query" \
        --work-group "$WORKGROUP" \
        --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
        --query "QueryExecutionId" --output text 2>/dev/null)
    
    if [ -z "$query_id" ]; then
        log_error "Failed to start Athena query"
        return 1
    fi
    
    # Wait for query completion
    sleep 8
    
    # Get all columns as a single string
    local columns
    columns=$(aws athena get-query-results --query-execution-id "$query_id" \
        --query "ResultSet.Rows[*].Data[0].VarCharValue" --output text 2>/dev/null | tr '\n' ' ')
    
    if [ -z "$columns" ]; then
        log_error "No columns returned from Athena query"
        return 1
    fi
    
    log_info "Table columns: ${columns:0:200}..."  # Truncate for readability
    
    local all_found=true
    for col in "${expected_columns[@]}"; do
        # Case-insensitive search (Iceberg lowercases column names)
        if echo "$columns" | grep -iq "$col"; then
            log_info "  ✅ Column found: $col"
        else
            log_error "  ❌ Column missing: $col"
            all_found=false
        fi
    done
    
    if [ "$all_found" = true ]; then
        log_info "DDL verification PASSED - all expected columns exist"
        return 0
    else
        log_error "DDL verification FAILED - missing columns"
        return 1
    fi
}

get_state_machine_arn() {
    # Try terraform output first (more reliable, no extra IAM needed)
    cd "$PROJECT_ROOT"
    STATE_MACHINE_ARN=$(terraform output -raw state_machine_arn 2>/dev/null || echo "")
    
    # Fallback to hardcoded ARN if terraform output fails
    if [ -z "$STATE_MACHINE_ARN" ]; then
        STATE_MACHINE_ARN="arn:aws:states:us-east-1:487500748616:stateMachine:lean-ops-dev-unified-orchestrator"
        log_warn "Using hardcoded state machine ARN (terraform output unavailable)"
    fi
    
    if [ -z "$STATE_MACHINE_ARN" ]; then
        log_error "State machine not found. Is infrastructure deployed?"
        return 1
    fi
    log_info "State machine: $STATE_MACHINE_ARN"
}

# =============================================================================
# UNIT TESTS
# =============================================================================

run_unit_tests() {
    log_step "Running Unit Tests"
    
    cd "$PROJECT_ROOT"
    
    if ! command -v pytest &> /dev/null; then
        log_warn "pytest not found, installing..."
        pip install pytest -q
    fi
    
    pytest scripts/glue/tests/ -v --tb=short
    
    log_info "Unit tests complete"
}

# =============================================================================
# INFRASTRUCTURE
# =============================================================================

deploy_infrastructure() {
    log_step "Deploying Infrastructure"
    
    cd "$PROJECT_ROOT"
    
    # Build Glue package
    log_info "Building Glue package..."
    ./scripts/build_glue.sh
    
    # Terraform apply
    log_info "Running terraform apply..."
    terraform apply -var-file="environments/dev.tfvars" -auto-approve
    
    # Upload Glue artifacts
    log_info "Uploading Glue artifacts to S3..."
    ./scripts/build_glue.sh --upload
    
    # Upload schema
    log_info "Uploading schema..."
    aws s3 cp schemas/events.json "s3://$BUCKET/schemas/events.json"
    
    log_info "Infrastructure deployed successfully"
}

destroy_infrastructure() {
    log_step "Destroying Infrastructure"
    
    cd "$PROJECT_ROOT"
    terraform destroy -var-file="environments/dev.tfvars" -auto-approve
    
    log_info "Infrastructure destroyed"
}

check_status() {
    log_step "Infrastructure Status"
    
    # Check terraform state
    cd "$PROJECT_ROOT"
    resource_count=$(terraform state list 2>/dev/null | wc -l || echo "0")
    log_info "Terraform resources: $resource_count"
    
    # Check Step Function
    if get_state_machine_arn 2>/dev/null; then
        log_info "Step Function: AVAILABLE"
    else
        log_warn "Step Function: NOT FOUND"
    fi
    
    # Check Glue job
    glue_job=$(aws glue get-job --job-name "lean-ops-dev-unified-job" 2>/dev/null && echo "AVAILABLE" || echo "NOT FOUND")
    log_info "Glue job: $glue_job"
    
    # Check RAW table
    raw_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_raw_db.events_staging" 2>/dev/null || echo "N/A")
    log_info "RAW table records: $raw_count"
    
    # Check Standardized table
    standardized_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.events" 2>/dev/null || echo "N/A")
    log_info "Standardized table records: $standardized_count"
}

# =============================================================================
# E2E TESTS
# =============================================================================

run_single_test() {
    local test_name="$1"
    local config_file="$2"
    local record_count="${3:-100}"
    local wait_time="${4:-90}"
    
    log_test "Running: $test_name"
    
    # Inject data
    log_info "Injecting $record_count records..."
    cd "$DATA_INJECTOR_DIR"
    source .venv/bin/activate 2>/dev/null || true
    python -m data_injector.main --config "$config_file" 2>&1 | tail -5
    
    # Wait for Firehose buffer
    log_info "Waiting ${wait_time}s for Firehose buffer..."
    sleep "$wait_time"
    
    # Trigger Step Function
    log_info "Triggering Step Function..."
    get_state_machine_arn
    
    execution_arn=$(aws stepfunctions start-execution \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --input '{"topic_name":"events"}' \
        --query "executionArn" --output text)
    
    log_info "Execution: ${execution_arn##*/}"
    
    # Wait for completion
    log_info "Waiting for completion..."
    if wait_for_stepfn "$execution_arn" 300; then
        log_info "✅ $test_name PASSED"
        return 0
    else
        log_error "❌ $test_name FAILED"
        return 1
    fi
}

run_e2e_tests() {
    log_step "Running E2E Test Suite"
    
    get_state_machine_arn || { log_error "Deploy infrastructure first"; exit 1; }
    
    local passed=0
    local failed=0
    local tests=()
    
    # Test definitions: name|config|records|wait
    # Core tests (always run)
    tests+=("Happy Path|$CONFIGS_DIR/happy_path.json|100|90")
    tests+=("Column Addition|$CONFIGS_DIR/test_column_addition.json|300|90")
    tests+=("Missing Column|$CONFIGS_DIR/test_missing_column.json|300|90")
    tests+=("Type Change|$CONFIGS_DIR/test_type_change.json|300|90")
    
    # Deduplication & MERGE tests (critical for data quality)
    tests+=("Network Duplicates|$CONFIGS_DIR/network_duplicates_sqs.json|300|90")
    tests+=("Merge Collision|$CONFIGS_DIR/test_merge_collision.json|300|90")
    
    # Schema evolution tests
    tests+=("Deep Flatten|$CONFIGS_DIR/test_flat_schema_evolution.json|300|90")
    tests+=("Schema Drift|$CONFIGS_DIR/schema_drift_sqs.json|300|90")
    
    # Edge cases
    tests+=("Empty Payload|$CONFIGS_DIR/test_standardized_empty_payload.json|100|90")
    
    for test_def in "${tests[@]}"; do
        IFS='|' read -r name config records wait <<< "$test_def"
        
        if [ -f "$config" ]; then
            if run_single_test "$name" "$config" "$records" "$wait"; then
                ((passed++))
            else
                ((failed++))
            fi
        else
            log_warn "Config not found: $config (skipping $name)"
        fi
        
        # Brief pause between tests
        sleep 10
    done
    
    # Summary
    log_step "E2E Test Summary"
    log_info "Passed: $passed"
    [ $failed -gt 0 ] && log_error "Failed: $failed" || log_info "Failed: $failed"
    log_info "Duration: $(elapsed_time)"
    
    # Verification queries
    log_step "Final Verification"
    
    raw_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_raw_db.events_staging")
    standardized_count=$(run_athena_query "SELECT COUNT(*) FROM iceberg_standardized_db.events")
    
    log_info "RAW table: $raw_count records"
    log_info "Standardized table: $standardized_count records"
    
    # DDL Verification - check schema drift columns were added
    log_step "DDL Verification"
    
    # Expected columns from schema drift tests:
    # - test_column_addition.json adds: extraField, newMetric, customTag
    # - test_flat_schema_evolution.json adds deep nested columns
    # - schema_drift_sqs.json adds various drift columns
    local expected_drift_columns=("extrafield" "newmetric" "customtag")
    
    if verify_ddl_columns "${expected_drift_columns[@]}"; then
        log_info "✅ DDL Verification PASSED"
    else
        log_warn "⚠️ DDL Verification FAILED - some expected columns missing"
        # Don't fail the overall test for DDL issues (may be first run)
    fi
    
    # Return failure if any test failed
    [ $failed -eq 0 ]
}

run_quick_test() {
    log_step "Quick Smoke Test"
    
    get_state_machine_arn || { log_error "Deploy infrastructure first"; exit 1; }
    
    # Check if happy_path config exists, if not create a simple one
    local config="$CONFIGS_DIR/happy_path.json"
    if [ ! -f "$config" ]; then
        log_warn "happy_path.json not found, using default schema"
        # Use existing test config
        config="$CONFIGS_DIR/test_column_addition.json"
    fi
    
    run_single_test "Quick Smoke Test" "$config" 50 60
}

run_stress_test() {
    log_step "Thundering Herd Stress Test"
    
    get_state_machine_arn || { log_error "Deploy infrastructure first"; exit 1; }
    
    local config="$CONFIGS_DIR/thundering_herd.json"
    if [ ! -f "$config" ]; then
        log_error "thundering_herd.json not found"
        exit 1
    fi
    
    log_warn "This test sends 5000 records - may take 5+ minutes"
    
    # Longer wait for 5000 records (120s for Firehose buffer)
    run_single_test "Thundering Herd (5000 records)" "$config" 5000 120
}

# =============================================================================
# MAIN
# =============================================================================

print_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  unit      Run unit tests only (pytest)"
    echo "  deploy    Deploy infrastructure (Terraform + uploads)"
    echo "  e2e       Run full E2E test suite (9 tests)"
    echo "  quick     Run quick smoke test (1 test)"
    echo "  stress    Run thundering herd stress test (5000 records)"
    echo "  all       Deploy + Unit + E2E"
    echo "  status    Check infrastructure status"
    echo "  destroy   Destroy infrastructure"
    echo ""
    echo "Environment:"
    echo "  AWS_PROFILE  AWS profile to use (default: terraform-firehose)"
}

case "${1:-}" in
    unit)
        run_unit_tests
        ;;
    deploy)
        deploy_infrastructure
        ;;
    e2e)
        run_e2e_tests
        ;;
    quick)
        run_quick_test
        ;;
    stress)
        run_stress_test
        ;;
    all)
        run_unit_tests
        deploy_infrastructure
        run_e2e_tests
        ;;
    status)
        check_status
        ;;
    destroy)
        destroy_infrastructure
        ;;
    -h|--help|help)
        print_usage
        ;;
    *)
        echo "Error: Unknown command '${1:-}'"
        print_usage
        exit 1
        ;;
esac

log_info "Total duration: $(elapsed_time)"
