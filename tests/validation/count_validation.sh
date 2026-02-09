#!/bin/bash
# =============================================================================
# COUNT VALIDATION SCRIPT
# =============================================================================
# Validates data accountability across pipeline layers (RAW → Standardized → Curated).
#
# Usage:
#   ./tests/validation/count_validation.sh all      # All layers (default)
#   ./tests/validation/count_validation.sh raw      # RAW only
#   ./tests/validation/count_validation.sh std      # Standardized only
#   ./tests/validation/count_validation.sh curated  # Curated only
#
# Environment:
#   AWS_PROFILE=terraform-firehose (default)
#   BUCKET=lean-ops-development-iceberg (default)
#
# Returns:
#   0 if all validations pass
#   1 if any validation fails
# =============================================================================

set -euo pipefail

# Configuration
BUCKET="${BUCKET:-lean-ops-development-iceberg}"
WORKGROUP="${WORKGROUP:-primary}"
export AWS_PROFILE="${AWS_PROFILE:-terraform-firehose}"

# Thresholds (from architecture proposal)
ACCOUNTABILITY_THRESHOLD=0.999  # 99.9%
PARSE_SUCCESS_THRESHOLD=0.995   # 99.5%
CDE_COMPLIANCE_THRESHOLD=0.98   # 98%

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Test results
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $1"; }
log_pass() { echo -e "${GREEN}[PASS]${NC} ✅ $1"; }
log_fail() { echo -e "${RED}[FAIL]${NC} ❌ $1"; }
log_section() { echo -e "\n${CYAN}========== $1 ==========${NC}\n"; }

# Run Athena query and return results
run_athena_query() {
    local query="$1"
    
    # Start query
    local query_id=$(aws athena start-query-execution \
        --query-string "$query" \
        --work-group "$WORKGROUP" \
        --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
        --query "QueryExecutionId" --output text 2>/dev/null)

    if [ -z "$query_id" ]; then
        echo "ERROR"
        return 1
    fi

    # Wait for completion
    local status="RUNNING"
    local max_wait=30
    local elapsed=0

    while [ "$status" = "RUNNING" ] || [ "$status" = "QUEUED" ]; do
        sleep 1
        elapsed=$((elapsed + 1))

        if [ $elapsed -gt $max_wait ]; then
            echo "ERROR"
            return 1
        fi

        status=$(aws athena get-query-execution \
            --query-execution-id "$query_id" \
            --query "QueryExecution.Status.State" --output text 2>/dev/null || echo "FAILED")
    done

    # Return results
    if [ "$status" = "SUCCEEDED" ]; then
        aws athena get-query-results \
            --query-execution-id "$query_id" \
            --query "ResultSet.Rows[1:].Data[0].VarCharValue" \
            --output text 2>/dev/null || echo "0"
    else
        echo "ERROR"
        return 1
    fi
}

# Get table count
get_table_count() {
    local table="$1"
    run_athena_query "SELECT COUNT(*) FROM $table" || echo "0"
}

# Record test result
record_test() {
    local test_name="$1"
    local result="$2"  # PASS or FAIL
    local details="${3:-}"

    ((TOTAL_TESTS++))

    if [ "$result" = "PASS" ]; then
        ((PASSED_TESTS++))
        log_pass "$test_name"
        [ -n "$details" ] && log_info "  $details"
    else
        ((FAILED_TESTS++))
        log_fail "$test_name"
        [ -n "$details" ] && log_error "  $details"
    fi
}

# =============================================================================
# VALIDATION: LAYER COUNTS
# =============================================================================

show_layer_counts() {
    log_section "DATA PIPELINE LAYER COUNTS"

    log_info "Querying table counts..."

    # Get counts
    local raw_count=$(get_table_count "iceberg_raw_db.events_staging")
    local std_count=$(get_table_count "iceberg_standardized_db.events")
    local curated_count=$(get_table_count "iceberg_curated_db.events")
    local parse_errors=$(get_table_count "iceberg_standardized_db.parse_errors")
    local cde_errors=$(get_table_count "iceberg_curated_db.errors")

    # Store for later validation
    export RAW_COUNT=$raw_count
    export STD_COUNT=$std_count
    export CURATED_COUNT=$curated_count
    export PARSE_ERRORS=$parse_errors
    export CDE_ERRORS=$cde_errors

    # Display table
    echo ""
    echo "┌─────────────────────────────────────────────┐"
    echo "│         DATA PIPELINE LAYER COUNTS         │"
    echo "├─────────────────────────────────────────────┤"
    printf "│ RAW (events_staging):       %10s     │\n" "$raw_count"
    printf "│ Standardized (events):      %10s     │\n" "$std_count"
    printf "│ Curated (events):           %10s     │\n" "$curated_count"
    echo "├─────────────────────────────────────────────┤"
    printf "│ Parse Errors:               %10s     │\n" "$parse_errors"
    printf "│ CDE Errors:                 %10s     │\n" "$cde_errors"
    echo "└─────────────────────────────────────────────┘"
    echo ""
}

# =============================================================================
# VALIDATION: RAW → STANDARDIZED ACCOUNTABILITY
# =============================================================================

validate_raw_to_standardized() {
    log_section "RAW → STANDARDIZED ACCOUNTABILITY"

    log_info "Testing: RAW count = Standardized count + Parse errors"
    log_info "Threshold: ≥ ${ACCOUNTABILITY_THRESHOLD} (99.9%)"
    echo ""

    # Get counts
    local raw_count=${RAW_COUNT:-$(get_table_count "iceberg_raw_db.events_staging")}
    local std_count=${STD_COUNT:-$(get_table_count "iceberg_standardized_db.events")}
    local parse_errors=${PARSE_ERRORS:-$(get_table_count "iceberg_standardized_db.parse_errors")}

    # Handle ERROR responses from Athena
    if [ "$raw_count" = "ERROR" ] || [ "$std_count" = "ERROR" ] || [ "$parse_errors" = "ERROR" ]; then
        log_error "Failed to query one or more tables (Athena query failed)"
        record_test "RAW → Standardized accountability" "FAIL" "Unable to query tables"
        return 1
    fi

    if [ "$raw_count" = "0" ]; then
        log_warn "RAW table is empty - skipping validation"
        return 0
    fi

    local accounted=$((std_count + parse_errors))
    local delta=$((raw_count - accounted))
    local accountability_pct=$(awk "BEGIN {printf \"%.4f\", $accounted / $raw_count}")

    log_test "RAW: $raw_count | Standardized: $std_count | Parse Errors: $parse_errors"
    log_test "Accounted: $accounted | Delta: $delta | Accountability: ${accountability_pct} ($(awk "BEGIN {printf \"%.2f\", $accountability_pct * 100}")%)"

    # Validate accountability threshold
    local threshold_check=$(awk "BEGIN {print ($accountability_pct >= $ACCOUNTABILITY_THRESHOLD)}")
    
    if [ "$threshold_check" = "1" ]; then
        record_test "RAW → Standardized accountability" "PASS" "Accountability: ${accountability_pct} ≥ ${ACCOUNTABILITY_THRESHOLD}"
    else
        record_test "RAW → Standardized accountability" "FAIL" "Accountability: ${accountability_pct} < ${ACCOUNTABILITY_THRESHOLD} (threshold breach)"
        log_error "  Missing: $delta records not accounted for"
    fi

    # Validate parse success rate
    if [ "$raw_count" -gt 0 ]; then
        local parse_success_pct=$(awk "BEGIN {printf \"%.4f\", $std_count / $raw_count}")
        local parse_threshold_check=$(awk "BEGIN {print ($parse_success_pct >= $PARSE_SUCCESS_THRESHOLD)}")
        
        log_test "Parse success rate: ${parse_success_pct} ($(awk "BEGIN {printf \"%.2f\", $parse_success_pct * 100}")%)"
        
        if [ "$parse_threshold_check" = "1" ]; then
            record_test "Parse success rate" "PASS" "Parse success: ${parse_success_pct} ≥ ${PARSE_SUCCESS_THRESHOLD}"
        else
            record_test "Parse success rate" "FAIL" "Parse success: ${parse_success_pct} < ${PARSE_SUCCESS_THRESHOLD} (threshold breach)"
            log_error "  Parse errors: $parse_errors records ($(awk "BEGIN {printf \"%.2f\", ($parse_errors / $raw_count) * 100}")%)"
        fi
    fi

    echo ""
}

# =============================================================================
# VALIDATION: STANDARDIZED → CURATED ACCOUNTABILITY
# =============================================================================

validate_standardized_to_curated() {
    log_section "STANDARDIZED → CURATED ACCOUNTABILITY"

    log_info "Testing: Standardized count = Curated count + CDE errors"
    log_info "Threshold: ≥ ${CDE_COMPLIANCE_THRESHOLD} (98%)"
    echo ""

    # Get counts
    local std_count=${STD_COUNT:-$(get_table_count "iceberg_standardized_db.events")}
    local curated_count=${CURATED_COUNT:-$(get_table_count "iceberg_curated_db.events")}
    local cde_errors=${CDE_ERRORS:-$(get_table_count "iceberg_curated_db.errors")}

    # Handle ERROR responses from Athena
    if [ "$std_count" = "ERROR" ] || [ "$curated_count" = "ERROR" ] || [ "$cde_errors" = "ERROR" ]; then
        log_error "Failed to query one or more tables (Athena query failed)"
        record_test "Standardized → Curated accountability" "FAIL" "Unable to query tables"
        return 1
    fi

    if [ "$std_count" = "0" ]; then
        log_warn "Standardized table is empty - skipping validation"
        return 0
    fi

    local accounted=$((curated_count + cde_errors))
    local delta=$((std_count - accounted))
    local accountability_pct=$(awk "BEGIN {printf \"%.4f\", $accounted / $std_count}")

    log_test "Standardized: $std_count | Curated: $curated_count | CDE Errors: $cde_errors"
    log_test "Accounted: $accounted | Delta: $delta | Accountability: ${accountability_pct} ($(awk "BEGIN {printf \"%.2f\", $accountability_pct * 100}")%)"

    # Validate accountability threshold
    local threshold_check=$(awk "BEGIN {print ($accountability_pct >= $ACCOUNTABILITY_THRESHOLD)}")
    
    if [ "$threshold_check" = "1" ]; then
        record_test "Standardized → Curated accountability" "PASS" "Accountability: ${accountability_pct} ≥ ${ACCOUNTABILITY_THRESHOLD}"
    else
        record_test "Standardized → Curated accountability" "FAIL" "Accountability: ${accountability_pct} < ${ACCOUNTABILITY_THRESHOLD} (threshold breach)"
        log_error "  Missing: $delta records not accounted for"
    fi

    # Validate CDE compliance rate
    if [ "$std_count" -gt 0 ]; then
        local cde_compliance_pct=$(awk "BEGIN {printf \"%.4f\", $curated_count / $std_count}")
        local cde_threshold_check=$(awk "BEGIN {print ($cde_compliance_pct >= $CDE_COMPLIANCE_THRESHOLD)}")
        
        log_test "CDE compliance rate: ${cde_compliance_pct} ($(awk "BEGIN {printf \"%.2f\", $cde_compliance_pct * 100}")%)"
        
        if [ "$cde_threshold_check" = "1" ]; then
            record_test "CDE compliance rate" "PASS" "CDE compliance: ${cde_compliance_pct} ≥ ${CDE_COMPLIANCE_THRESHOLD}"
        else
            record_test "CDE compliance rate" "FAIL" "CDE compliance: ${cde_compliance_pct} < ${CDE_COMPLIANCE_THRESHOLD} (threshold breach)"
            log_error "  CDE violations: $cde_errors records ($(awk "BEGIN {printf \"%.2f\", ($cde_errors / $std_count) * 100}")%)"
        fi
    fi

    echo ""
}

# =============================================================================
# VALIDATION: CDE VIOLATION SPIKE DETECTION
# =============================================================================

validate_cde_spike_detection() {
    log_section "CDE VIOLATION SPIKE DETECTION"

    log_info "Testing: CDE violation rate < 10% (spike threshold)"
    echo ""

    local std_count=${STD_COUNT:-$(get_table_count "iceberg_standardized_db.events")}
    local cde_errors=${CDE_ERRORS:-$(get_table_count "iceberg_curated_db.errors")}

    # Handle ERROR responses from Athena
    if [ "$std_count" = "ERROR" ] || [ "$cde_errors" = "ERROR" ]; then
        log_error "Failed to query one or more tables (Athena query failed)"
        record_test "CDE violation spike detection" "FAIL" "Unable to query tables"
        return 1
    fi

    if [ "$std_count" = "0" ]; then
        log_warn "Standardized table is empty - skipping validation"
        return 0
    fi

    local error_rate=$(awk "BEGIN {printf \"%.4f\", $cde_errors / $std_count}")
    local spike_threshold=0.10

    log_test "CDE errors: $cde_errors | Total: $std_count | Error rate: ${error_rate} ($(awk "BEGIN {printf \"%.2f\", $error_rate * 100}")%)"

    local spike_check=$(awk "BEGIN {print ($error_rate < $spike_threshold)}")
    
    if [ "$spike_check" = "1" ]; then
        record_test "CDE violation spike detection" "PASS" "Error rate ${error_rate} < ${spike_threshold} (no spike detected)"
    else
        record_test "CDE violation spike detection" "FAIL" "Error rate ${error_rate} ≥ ${spike_threshold} (SPIKE DETECTED - investigate source system)"
        log_error "  ⚠️  CRITICAL: CDE violation rate exceeds 10% threshold"
        log_error "  Action: Check source system for data quality issues"
    fi

    echo ""
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

print_usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  all        Run all validations (default)"
    echo "  raw        Validate RAW → Standardized only"
    echo "  std        Validate Standardized → Curated only"
    echo "  curated    Validate Curated layer only"
    echo "  spike      Validate CDE spike detection only"
    echo ""
    echo "Environment:"
    echo "  AWS_PROFILE  AWS profile (default: terraform-firehose)"
    echo "  BUCKET       S3 bucket (default: lean-ops-development-iceberg)"
}

main() {
    local command="${1:-all}"

    case "$command" in
        raw)
            show_layer_counts
            validate_raw_to_standardized
            ;;
        std)
            show_layer_counts
            validate_standardized_to_curated
            ;;
        curated)
            show_layer_counts
            validate_standardized_to_curated
            validate_cde_spike_detection
            ;;
        spike)
            show_layer_counts
            validate_cde_spike_detection
            ;;
        all)
            show_layer_counts
            validate_raw_to_standardized
            validate_standardized_to_curated
            validate_cde_spike_detection
            ;;
        -h|--help|help)
            print_usage
            exit 0
            ;;
        *)
            log_error "Unknown command: $command"
            print_usage
            exit 1
            ;;
    esac

    # Summary
    if [ $TOTAL_TESTS -gt 0 ]; then
        log_section "VALIDATION SUMMARY"
        log_info "Total tests: $TOTAL_TESTS"
        log_info "Passed: $PASSED_TESTS"
        [ $FAILED_TESTS -gt 0 ] && log_error "Failed: $FAILED_TESTS" || log_info "Failed: $FAILED_TESTS"

        if [ $FAILED_TESTS -eq 0 ]; then
            echo ""
            log_pass "ALL COUNT VALIDATIONS PASSED ✅"
            echo ""
            exit 0
        else
            echo ""
            log_fail "COUNT VALIDATION FAILED ❌"
            echo ""
            log_error "Review the failure details above and check:"
            log_error "  1. Data pipeline for missing records"
            log_error "  2. Parse error rates in CloudWatch logs"
            log_error "  3. CDE validation logic in curated_processor.py"
            exit 1
        fi
    fi
}

main "$@"
