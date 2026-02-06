#!/bin/bash
# =============================================================================
# PHASE 1 UNIT TESTS: Schema Registry DynamoDB Flag
# =============================================================================
# Tests the DynamoDB-based processing flag mechanism
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
NC='\033[0m'

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

# Get table name from Terraform
TABLE_NAME=$(terraform output -raw dynamodb_tables 2>/dev/null | jq -r '.schema_registry // empty' || echo "lean-ops-dev-schema-registry")

if [ -z "$TABLE_NAME" ]; then
    TABLE_NAME="lean-ops-dev-schema-registry"
fi

echo "Using schema registry table: $TABLE_NAME"

PASSED=0
FAILED=0

log_pass() { echo -e "${GREEN}✅ PASS:${NC} $1"; ((PASSED++)); }
log_fail() { echo -e "${RED}❌ FAIL:${NC} $1"; ((FAILED++)); }
log_info() { echo -e "${YELLOW}ℹ️  INFO:${NC} $1"; }

# =============================================================================
# TEST 1: Table exists
# =============================================================================
test_table_exists() {
    log_info "Testing table exists..."
    if aws dynamodb describe-table --table-name "$TABLE_NAME" &>/dev/null; then
        log_pass "Schema registry table exists"
    else
        log_fail "Schema registry table does not exist"
        return 1
    fi
}

# =============================================================================
# TEST 2: Can write and read processing flag
# =============================================================================
test_write_read_flag() {
    log_info "Testing write/read processing flag..."
    
    # Write a test topic
    aws dynamodb put-item \
        --table-name "$TABLE_NAME" \
        --item '{
            "topic_name": {"S": "test_topic"},
            "processing_enabled": {"BOOL": true},
            "status": {"S": "READY"},
            "updated_at": {"S": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}
        }' 2>/dev/null
    
    # Read it back
    RESULT=$(aws dynamodb get-item \
        --table-name "$TABLE_NAME" \
        --key '{"topic_name": {"S": "test_topic"}}' \
        --query 'Item.processing_enabled.BOOL' \
        --output text 2>/dev/null)
    
    if [ "$RESULT" == "True" ]; then
        log_pass "Can write and read processing_enabled=true"
    else
        log_fail "Failed to read processing_enabled flag (got: $RESULT)"
        return 1
    fi
}

# =============================================================================
# TEST 3: Can toggle processing flag
# =============================================================================
test_toggle_flag() {
    log_info "Testing toggle processing flag..."
    
    # Disable processing
    aws dynamodb update-item \
        --table-name "$TABLE_NAME" \
        --key '{"topic_name": {"S": "test_topic"}}' \
        --update-expression "SET processing_enabled = :val, #s = :status" \
        --expression-attribute-names '{"#s": "status"}' \
        --expression-attribute-values '{":val": {"BOOL": false}, ":status": {"S": "MAINTENANCE"}}' 2>/dev/null
    
    # Read it back
    RESULT=$(aws dynamodb get-item \
        --table-name "$TABLE_NAME" \
        --key '{"topic_name": {"S": "test_topic"}}' \
        --query 'Item.processing_enabled.BOOL' \
        --output text 2>/dev/null)
    
    if [ "$RESULT" == "False" ]; then
        log_pass "Can toggle processing_enabled to false"
    else
        log_fail "Failed to toggle processing_enabled flag (got: $RESULT)"
        return 1
    fi
    
    # Re-enable
    aws dynamodb update-item \
        --table-name "$TABLE_NAME" \
        --key '{"topic_name": {"S": "test_topic"}}' \
        --update-expression "SET processing_enabled = :val, #s = :status" \
        --expression-attribute-names '{"#s": "status"}' \
        --expression-attribute-values '{":val": {"BOOL": true}, ":status": {"S": "READY"}}' 2>/dev/null
    
    log_pass "Can toggle processing_enabled back to true"
}

# =============================================================================
# TEST 4: Non-existent topic returns empty
# =============================================================================
test_nonexistent_topic() {
    log_info "Testing non-existent topic..."
    
    RESULT=$(aws dynamodb get-item \
        --table-name "$TABLE_NAME" \
        --key '{"topic_name": {"S": "nonexistent_topic_xyz123"}}' \
        --query 'Item' \
        --output text 2>/dev/null)
    
    if [ "$RESULT" == "None" ] || [ -z "$RESULT" ]; then
        log_pass "Non-existent topic returns empty"
    else
        log_fail "Non-existent topic should return empty (got: $RESULT)"
        return 1
    fi
}

# =============================================================================
# TEST 5: Cleanup test data
# =============================================================================
cleanup() {
    log_info "Cleaning up test data..."
    aws dynamodb delete-item \
        --table-name "$TABLE_NAME" \
        --key '{"topic_name": {"S": "test_topic"}}' 2>/dev/null || true
    log_pass "Cleanup completed"
}

# =============================================================================
# RUN TESTS
# =============================================================================
echo ""
echo "=========================================="
echo "PHASE 1 UNIT TESTS: Schema Registry"
echo "=========================================="
echo ""

test_table_exists
test_write_read_flag
test_toggle_flag
test_nonexistent_topic
cleanup

echo ""
echo "=========================================="
echo "RESULTS: Passed: $PASSED, Failed: $FAILED"
echo "=========================================="

if [ $FAILED -gt 0 ]; then
    exit 1
fi
