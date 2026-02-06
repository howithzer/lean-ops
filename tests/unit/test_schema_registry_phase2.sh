#!/bin/bash
# =============================================================================
# PHASE 2 UNIT TESTS: Schema Registration Workflow
# =============================================================================
# Tests the Phase 2 schema management workflow:
# - S3 folder structure creation
# - Schema registration (pending → active flow)
# - DynamoDB flag updates
# - Invalid schema handling
#
# Prerequisites:
#   - terraform apply completed
#   - AWS credentials configured
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

# AWS Configuration (match E2E test settings)
export AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}"

# Configuration (can be overridden by environment)
BUCKET="${SCHEMA_BUCKET:-lean-ops-development-iceberg}"
TABLE_NAME="${SCHEMA_REGISTRY_TABLE:-lean-ops-dev-schema-registry}"
TEST_TOPIC="test_schema_phase2"

PASSED=0
FAILED=0

log_pass() { echo -e "${GREEN}✅ PASS:${NC} $1"; PASSED=$((PASSED + 1)); }
log_fail() { echo -e "${RED}❌ FAIL:${NC} $1"; FAILED=$((FAILED + 1)); }
log_info() { echo -e "${YELLOW}ℹ️  INFO:${NC} $1"; }
log_test() { echo -e "${CYAN}🧪 TEST:${NC} $1"; }

# =============================================================================
# SETUP: Create temp test schema
# =============================================================================
setup() {
    log_info "Setting up test environment..."
    
    # Create a valid test schema
    mkdir -p "$PROJECT_ROOT/tests/schemas"
    cat > "$PROJECT_ROOT/tests/schemas/${TEST_TOPIC}.json" <<EOF
{
    "schema_version": "1.0.0",
    "table_name": "${TEST_TOPIC}",
    "description": "Test schema for Phase 2 unit tests",
    "envelope_columns": {
        "message_id": {"type": "STRING", "required": true},
        "idempotency_key": {"type": "STRING", "required": true}
    },
    "payload_columns": {
        "test_field": {"json_path": "$.test"}
    }
}
EOF

    # Create an invalid test schema (missing required fields)
    cat > "$PROJECT_ROOT/tests/schemas/${TEST_TOPIC}_invalid.json" <<EOF
{
    "schema_version": "1.0.0",
    "description": "Invalid schema - missing table_name"
}
EOF

    export SCHEMA_BUCKET="$BUCKET"
    export SCHEMA_REGISTRY_TABLE="$TABLE_NAME"
    
    log_info "Test schemas created"
}

# =============================================================================
# TEST 1: S3 folder structure creation
# =============================================================================
test_s3_folder_structure() {
    log_test "Testing S3 folder structure creation..."
    
    # Run register_schema.sh (this creates the folder structure)
    "$PROJECT_ROOT/scripts/register_schema.sh" "$TEST_TOPIC" "$PROJECT_ROOT/tests/schemas/${TEST_TOPIC}.json" >/dev/null 2>&1
    
    # Check that folders exist (via .keep files)
    local folders_found=0
    for folder in pending active failed archive; do
        if aws s3 ls "s3://${BUCKET}/schemas/${TEST_TOPIC}/${folder}/.keep" >/dev/null 2>&1; then
            ((folders_found++))
        fi
    done
    
    # Check for active schema
    if aws s3 ls "s3://${BUCKET}/schemas/${TEST_TOPIC}/active/schema.json" >/dev/null 2>&1; then
        log_pass "S3 folder structure created with active schema"
    else
        log_fail "S3 folder structure incomplete (expected 4 folders + active schema)"
        return 1
    fi
}

# =============================================================================
# TEST 2: Schema registration (pending → active flow)
# =============================================================================
test_schema_registration() {
    log_test "Testing schema registration flow..."
    
    # Unregister first to reset state
    "$PROJECT_ROOT/scripts/unregister_schema.sh" "$TEST_TOPIC" --remove-schema >/dev/null 2>&1 || true
    
    # Register with valid schema
    "$PROJECT_ROOT/scripts/register_schema.sh" "$TEST_TOPIC" "$PROJECT_ROOT/tests/schemas/${TEST_TOPIC}.json" >/dev/null 2>&1
    
    # Check schema is in active/ (not pending/)
    local active_exists="no"
    local pending_exists="no"
    aws s3 ls "s3://${BUCKET}/schemas/${TEST_TOPIC}/active/schema.json" >/dev/null 2>&1 && active_exists="yes"
    aws s3 ls "s3://${BUCKET}/schemas/${TEST_TOPIC}/pending/schema.json" >/dev/null 2>&1 && pending_exists="yes"
    
    if [ "$active_exists" = "yes" ] && [ "$pending_exists" = "no" ]; then
        log_pass "Schema moved from pending/ to active/"
    else
        log_fail "Schema not properly moved (active=$active_exists, pending=$pending_exists)"
        return 1
    fi
}

# =============================================================================
# TEST 3: DynamoDB flag update on registration
# =============================================================================
test_dynamodb_flag_update() {
    log_test "Testing DynamoDB flag update..."
    
    # Get the flag from DynamoDB
    local result=$(aws dynamodb get-item \
        --table-name "$TABLE_NAME" \
        --key "{\"topic_name\": {\"S\": \"${TEST_TOPIC}\"}}" \
        --projection-expression "processing_enabled, #s" \
        --expression-attribute-names '{"#s": "status"}' \
        --query 'Item' \
        --output json 2>/dev/null)
    
    local enabled=$(echo "$result" | jq -r '.processing_enabled.BOOL // empty')
    local status=$(echo "$result" | jq -r '.status.S // empty')
    
    if [ "$enabled" = "true" ] && [ "$status" = "READY" ]; then
        log_pass "DynamoDB processing_enabled=true, status=READY"
    else
        log_fail "DynamoDB not updated correctly (enabled=$enabled, status=$status)"
        return 1
    fi
}

# =============================================================================
# TEST 4: Schema unregistration (flag disable)
# =============================================================================
test_schema_unregistration() {
    log_test "Testing schema unregistration..."
    
    # Unregister
    "$PROJECT_ROOT/scripts/unregister_schema.sh" "$TEST_TOPIC" >/dev/null 2>&1
    
    # Check DynamoDB flag
    local enabled=$(aws dynamodb get-item \
        --table-name "$TABLE_NAME" \
        --key "{\"topic_name\": {\"S\": \"${TEST_TOPIC}\"}}" \
        --query 'Item.processing_enabled.BOOL' \
        --output text 2>/dev/null)
    
    if [ "$enabled" = "False" ]; then
        log_pass "DynamoDB processing_enabled=false after unregister"
    else
        log_fail "Unregister did not disable flag (enabled=$enabled)"
        return 1
    fi
}

# =============================================================================
# TEST 5: Invalid schema handling
# =============================================================================
test_invalid_schema_handling() {
    log_test "Testing invalid schema handling..."
    
    # Try to register invalid schema (should fail)
    if "$PROJECT_ROOT/scripts/register_schema.sh" "${TEST_TOPIC}_invalid" "$PROJECT_ROOT/tests/schemas/${TEST_TOPIC}_invalid.json" >/dev/null 2>&1; then
        log_fail "Invalid schema was accepted (should have failed)"
        return 1
    fi
    
    # Check error.json exists
    local error_exists="no"
    aws s3 ls "s3://${BUCKET}/schemas/${TEST_TOPIC}_invalid/failed/error.json" >/dev/null 2>&1 && error_exists="yes"
    
    # Check DynamoDB status is ERROR
    local status=$(aws dynamodb get-item \
        --table-name "$TABLE_NAME" \
        --key "{\"topic_name\": {\"S\": \"${TEST_TOPIC}_invalid\"}}" \
        --query 'Item.status.S' \
        --output text 2>/dev/null)
    
    if [ "$error_exists" = "yes" ] && [ "$status" = "ERROR" ]; then
        log_pass "Invalid schema moved to failed/ with error.json, DynamoDB status=ERROR"
    else
        log_fail "Invalid schema not handled correctly (error_exists=$error_exists, status=$status)"
        return 1
    fi
}

# =============================================================================
# CLEANUP
# =============================================================================
cleanup() {
    log_info "Cleaning up test data..."
    
    # Remove test topics from DynamoDB
    aws dynamodb delete-item \
        --table-name "$TABLE_NAME" \
        --key "{\"topic_name\": {\"S\": \"${TEST_TOPIC}\"}}" 2>/dev/null || true
    aws dynamodb delete-item \
        --table-name "$TABLE_NAME" \
        --key "{\"topic_name\": {\"S\": \"${TEST_TOPIC}_invalid\"}}" 2>/dev/null || true
    
    # Remove test schemas from S3
    aws s3 rm "s3://${BUCKET}/schemas/${TEST_TOPIC}/" --recursive 2>/dev/null || true
    aws s3 rm "s3://${BUCKET}/schemas/${TEST_TOPIC}_invalid/" --recursive 2>/dev/null || true
    
    # Remove local test files
    rm -f "$PROJECT_ROOT/tests/schemas/${TEST_TOPIC}.json"
    rm -f "$PROJECT_ROOT/tests/schemas/${TEST_TOPIC}_invalid.json"
    
    log_pass "Cleanup completed"
}

# =============================================================================
# RUN TESTS
# =============================================================================
echo ""
echo "=========================================="
echo "PHASE 2 UNIT TESTS: Schema Registration"
echo "=========================================="
echo ""

setup
test_s3_folder_structure
test_schema_registration
test_dynamodb_flag_update
test_schema_unregistration
test_invalid_schema_handling
cleanup

echo ""
echo "=========================================="
echo "RESULTS: Passed: $PASSED, Failed: $FAILED"
echo "=========================================="

if [ $FAILED -gt 0 ]; then
    exit 1
fi
