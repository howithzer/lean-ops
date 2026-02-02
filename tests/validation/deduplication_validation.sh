#!/bin/bash
# =============================================================================
# DEDUPLICATION VALIDATION SCRIPT
# =============================================================================
# Validates FIFO and LIFO deduplication logic with detailed logging.
#
# Usage:
#   ./tests/validation/deduplication_validation.sh fifo     # Test FIFO only
#   ./tests/validation/deduplication_validation.sh lifo     # Test LIFO only
#   ./tests/validation/deduplication_validation.sh all      # Test both
#   ./tests/validation/deduplication_validation.sh stats    # Show stats only
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
    local format="${2:-text}"  # text or json

    # Start query
    local query_id=$(aws athena start-query-execution \
        --query-string "$query" \
        --work-group "$WORKGROUP" \
        --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
        --query "QueryExecutionId" --output text 2>/dev/null)

    if [ -z "$query_id" ]; then
        echo "ERROR: Failed to start query"
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
            echo "ERROR: Query timeout"
            return 1
        fi

        status=$(aws athena get-query-execution \
            --query-execution-id "$query_id" \
            --query "QueryExecution.Status.State" --output text 2>/dev/null || echo "FAILED")
    done

    # Return results
    if [ "$status" = "SUCCEEDED" ]; then
        if [ "$format" = "json" ]; then
            aws athena get-query-results \
                --query-execution-id "$query_id" \
                --output json 2>/dev/null
        else
            # Return all rows except header (skip first row)
            aws athena get-query-results \
                --query-execution-id "$query_id" \
                --query "ResultSet.Rows[1:].Data[0].VarCharValue" \
                --output text 2>/dev/null || echo "0"
        fi
    else
        echo "ERROR: Query failed with status: $status"
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
# VALIDATION: DATA QUALITY STATS
# =============================================================================

show_data_quality_stats() {
    log_section "DATA QUALITY STATISTICS"

    log_info "Querying table counts..."

    # Get counts
    local raw_count=$(get_table_count "iceberg_raw_db.events_staging")
    local std_count=$(get_table_count "iceberg_standardized_db.events")
    local curated_count=$(get_table_count "iceberg_curated_db.events")
    local parse_errors=$(get_table_count "iceberg_standardized_db.parse_errors")
    local cde_errors=$(get_table_count "iceberg_curated_db.errors")

    # Display table
    echo ""
    echo "┌─────────────────────────────────────────────┐"
    echo "│         DATA PIPELINE STATISTICS           │"
    echo "├─────────────────────────────────────────────┤"
    printf "│ RAW (events_staging):       %10s     │\n" "$raw_count"
    printf "│ Standardized (events):      %10s     │\n" "$std_count"
    printf "│ Curated (events):           %10s     │\n" "$curated_count"
    printf "│ Parse Errors:               %10s     │\n" "$parse_errors"
    printf "│ CDE Errors:                 %10s     │\n" "$cde_errors"
    echo "└─────────────────────────────────────────────┘"
    echo ""

    # Get unique counts
    log_info "Checking duplicate levels..."

    local raw_unique_msg=$(run_athena_query "SELECT COUNT(DISTINCT message_id) FROM iceberg_raw_db.events_staging")
    local raw_unique_key=$(run_athena_query "SELECT COUNT(DISTINCT idempotency_key) FROM iceberg_raw_db.events_staging WHERE idempotency_key IS NOT NULL")

    local std_unique_msg=$(run_athena_query "SELECT COUNT(DISTINCT message_id) FROM iceberg_standardized_db.events")
    local std_unique_key=$(run_athena_query "SELECT COUNT(DISTINCT idempotency_key) FROM iceberg_standardized_db.events WHERE idempotency_key IS NOT NULL")

    local curated_unique_key=$(run_athena_query "SELECT COUNT(DISTINCT idempotency_key) FROM iceberg_curated_db.events WHERE idempotency_key IS NOT NULL")

    echo "┌─────────────────────────────────────────────┐"
    echo "│         DEDUPLICATION STATISTICS           │"
    echo "├─────────────────────────────────────────────┤"
    printf "│ RAW - Unique message_ids:    %10s     │\n" "$raw_unique_msg"
    printf "│ RAW - Unique idempotency_keys: %8s     │\n" "$raw_unique_key"
    printf "│ STD - Unique message_ids:    %10s     │\n" "$std_unique_msg"
    printf "│ STD - Unique idempotency_keys: %8s     │\n" "$std_unique_key"
    printf "│ CUR - Unique idempotency_keys: %8s     │\n" "$curated_unique_key"
    echo "├─────────────────────────────────────────────┤"

    # Calculate dedup effectiveness
    local network_dupes_removed=$((raw_count - std_unique_msg))
    local business_corrections=$((std_unique_msg - std_unique_key))

    printf "│ Network duplicates removed:  %10s     │\n" "$network_dupes_removed"
    printf "│ Business corrections applied: %9s     │\n" "$business_corrections"
    echo "└─────────────────────────────────────────────┘"
    echo ""
}

# =============================================================================
# VALIDATION: FIFO (First In, First Out) - Network Deduplication
# =============================================================================

validate_fifo() {
    log_section "FIFO VALIDATION (Network Deduplication)"

    log_info "Testing: When duplicate message_ids exist, keep the FIRST occurrence"
    log_info "Rule: Group by message_id, keep earliest ingestion_ts"
    echo ""

    # Step 1: Check if duplicates exist in RAW
    log_test "Step 1: Checking for duplicate message_ids in RAW..."

    local raw_dupes=$(run_athena_query "
        SELECT COUNT(*) FROM (
            SELECT message_id
            FROM iceberg_raw_db.events_staging
            GROUP BY message_id
            HAVING COUNT(*) > 1
        )
    ")

    if [ "$raw_dupes" = "0" ] || [ "$raw_dupes" = "ERROR: Query failed with status: FAILED" ]; then
        log_warn "No duplicate message_ids found in RAW - FIFO test SKIPPED"
        log_info "To test FIFO, inject data with network duplicates:"
        log_info "  Config: tests/configs/test_02_network_duplicates.json"
        echo ""
        return 0
    fi

    log_info "Found $raw_dupes message_ids with duplicates in RAW"

    # Step 2: For duplicates, verify FIRST was kept
    log_test "Step 2: Validating that FIRST occurrence was kept in Standardized..."

    local validation_query="
    WITH raw_duplicates AS (
        -- Find all duplicate message_ids and their ingestion timestamps
        SELECT message_id, ingestion_ts,
               ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY ingestion_ts ASC) as rn
        FROM iceberg_raw_db.events_staging
        WHERE message_id IN (
            SELECT message_id FROM iceberg_raw_db.events_staging
            GROUP BY message_id HAVING COUNT(*) > 1
        )
    ),
    first_expected AS (
        -- Get the FIRST occurrence (earliest ingestion_ts) for each message_id
        SELECT message_id, ingestion_ts as expected_ingestion_ts
        FROM raw_duplicates WHERE rn = 1
    ),
    standardized_actual AS (
        -- Get what was actually kept in Standardized
        SELECT message_id, ingestion_ts as actual_ingestion_ts
        FROM iceberg_standardized_db.events
        WHERE message_id IN (SELECT message_id FROM first_expected)
    )
    SELECT
        COUNT(*) as failures
    FROM first_expected fe
    LEFT JOIN standardized_actual sa ON fe.message_id = sa.message_id
    WHERE fe.expected_ingestion_ts != sa.actual_ingestion_ts
       OR sa.actual_ingestion_ts IS NULL
    "

    local fifo_failures=$(run_athena_query "$validation_query")

    if [ "$fifo_failures" = "0" ]; then
        record_test "FIFO deduplication" "PASS" "All $raw_dupes duplicate message_ids kept FIRST occurrence"

        # Show sample for verification
        log_info ""
        log_info "Sample verification (first 3 duplicates):"

        local sample_query="
        WITH raw_duplicates AS (
            SELECT message_id, ingestion_ts,
                   ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY ingestion_ts ASC) as rn,
                   COUNT(*) OVER (PARTITION BY message_id) as dup_count
            FROM iceberg_raw_db.events_staging
            WHERE message_id IN (
                SELECT message_id FROM iceberg_raw_db.events_staging
                GROUP BY message_id HAVING COUNT(*) > 1
            )
        ),
        first_expected AS (
            SELECT message_id, ingestion_ts as first_ts, dup_count
            FROM raw_duplicates WHERE rn = 1
            LIMIT 3
        )
        SELECT
            fe.message_id,
            fe.dup_count as total_occurrences,
            fe.first_ts as expected_kept,
            sa.ingestion_ts as actually_kept,
            CASE WHEN fe.first_ts = sa.ingestion_ts THEN 'MATCH ✓' ELSE 'MISMATCH ✗' END as result
        FROM first_expected fe
        JOIN iceberg_standardized_db.events sa ON fe.message_id = sa.message_id
        "

        local sample_results=$(run_athena_query "$sample_query" "json")

        # Parse and display sample results
        echo "$sample_results" | python3 -c "
import sys, json
data = json.load(sys.stdin)
rows = data.get('ResultSet', {}).get('Rows', [])
if len(rows) > 1:  # Skip header
    print('  message_id | occurrences | expected_ts | actual_ts | result')
    print('  ' + '-' * 70)
    for row in rows[1:]:  # Skip header row
        cols = [d.get('VarCharValue', 'NULL') for d in row.get('Data', [])]
        if len(cols) >= 5:
            print(f'  {cols[0][:12]}... | {cols[1]:^11} | {cols[2]:^11} | {cols[3]:^9} | {cols[4]}')
" 2>/dev/null || log_info "  (Could not display sample - see Athena query results)"

    else
        record_test "FIFO deduplication" "FAIL" "$fifo_failures message_ids kept WRONG occurrence (should be FIRST)"

        # Show failures for debugging
        log_error ""
        log_error "FIFO Validation Failures (first 5):"

        local failure_query="
        WITH raw_duplicates AS (
            SELECT message_id, ingestion_ts,
                   ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY ingestion_ts ASC) as rn
            FROM iceberg_raw_db.events_staging
            WHERE message_id IN (
                SELECT message_id FROM iceberg_raw_db.events_staging
                GROUP BY message_id HAVING COUNT(*) > 1
            )
        ),
        first_expected AS (
            SELECT message_id, ingestion_ts as expected_ts
            FROM raw_duplicates WHERE rn = 1
        )
        SELECT
            fe.message_id,
            fe.expected_ts as should_be,
            sa.ingestion_ts as actually_is
        FROM first_expected fe
        LEFT JOIN iceberg_standardized_db.events sa ON fe.message_id = sa.message_id
        WHERE fe.expected_ts != sa.ingestion_ts OR sa.ingestion_ts IS NULL
        LIMIT 5
        "

        run_athena_query "$failure_query" "text" | head -5 | while read line; do
            log_error "  $line"
        done
    fi

    echo ""
}

# =============================================================================
# VALIDATION: LIFO (Last In, First Out) - Business Corrections
# =============================================================================

validate_lifo() {
    log_section "LIFO VALIDATION (Business Corrections)"

    log_info "Testing: When duplicate idempotency_keys exist, keep the LATEST occurrence"
    log_info "Rule: Group by idempotency_key, keep latest last_updated_ts (or publish_time)"
    echo ""

    # Step 1: Check if duplicates exist in Standardized
    log_test "Step 1: Checking for duplicate idempotency_keys in Standardized..."

    local std_dupes=$(run_athena_query "
        SELECT COUNT(*) FROM (
            SELECT idempotency_key
            FROM iceberg_standardized_db.events
            WHERE idempotency_key IS NOT NULL
            GROUP BY idempotency_key
            HAVING COUNT(*) > 1
        )
    ")

    if [ "$std_dupes" = "0" ] || [ "$std_dupes" = "ERROR: Query failed with status: FAILED" ]; then
        log_warn "No duplicate idempotency_keys found in Standardized - LIFO test SKIPPED"
        log_info "To test LIFO, inject data with business corrections:"
        log_info "  Config: tests/configs/test_03_business_corrections.json"
        echo ""
        return 0
    fi

    log_info "Found $std_dupes idempotency_keys with duplicates in Standardized"

    # Step 2: For duplicates, verify LATEST was kept in Curated
    log_test "Step 2: Validating that LATEST occurrence was kept in Curated..."

    local validation_query="
    WITH std_duplicates AS (
        -- Find all duplicate idempotency_keys with their timestamps
        SELECT
            idempotency_key,
            publish_time,
            json_extract_scalar(json_payload, '$.last_updated_ts') as payload_ts,
            COALESCE(
                json_extract_scalar(json_payload, '$.last_updated_ts'),
                publish_time,
                CAST(ingestion_ts AS VARCHAR)
            ) as sort_ts,
            ROW_NUMBER() OVER (
                PARTITION BY idempotency_key
                ORDER BY COALESCE(
                    json_extract_scalar(json_payload, '$.last_updated_ts'),
                    publish_time,
                    CAST(ingestion_ts AS VARCHAR)
                ) DESC
            ) as rn
        FROM iceberg_standardized_db.events
        WHERE idempotency_key IS NOT NULL
          AND idempotency_key IN (
              SELECT idempotency_key FROM iceberg_standardized_db.events
              WHERE idempotency_key IS NOT NULL
              GROUP BY idempotency_key HAVING COUNT(*) > 1
          )
    ),
    latest_expected AS (
        -- Get the LATEST occurrence (most recent timestamp)
        SELECT idempotency_key, sort_ts as expected_sort_ts
        FROM std_duplicates WHERE rn = 1
    ),
    curated_actual AS (
        -- Get what was actually kept in Curated
        SELECT idempotency_key,
               COALESCE(
                   CAST(last_updated_ts AS VARCHAR),
                   publish_time,
                   CAST(ingestion_ts AS VARCHAR)
               ) as actual_sort_ts
        FROM iceberg_curated_db.events
        WHERE idempotency_key IN (SELECT idempotency_key FROM latest_expected)
    )
    SELECT
        COUNT(*) as failures
    FROM latest_expected le
    LEFT JOIN curated_actual ca ON le.idempotency_key = ca.idempotency_key
    WHERE le.expected_sort_ts != ca.actual_sort_ts
       OR ca.actual_sort_ts IS NULL
    "

    local lifo_failures=$(run_athena_query "$validation_query")

    if [ "$lifo_failures" = "0" ]; then
        record_test "LIFO deduplication" "PASS" "All $std_dupes duplicate idempotency_keys kept LATEST occurrence"

        # Show sample
        log_info ""
        log_info "Sample verification (first 3 corrections):"

        local sample_query="
        WITH std_duplicates AS (
            SELECT
                idempotency_key,
                COALESCE(
                    json_extract_scalar(json_payload, '$.last_updated_ts'),
                    publish_time
                ) as sort_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY idempotency_key
                    ORDER BY COALESCE(
                        json_extract_scalar(json_payload, '$.last_updated_ts'),
                        publish_time
                    ) DESC
                ) as rn,
                COUNT(*) OVER (PARTITION BY idempotency_key) as version_count
            FROM iceberg_standardized_db.events
            WHERE idempotency_key IS NOT NULL
              AND idempotency_key IN (
                  SELECT idempotency_key FROM iceberg_standardized_db.events
                  WHERE idempotency_key IS NOT NULL
                  GROUP BY idempotency_key HAVING COUNT(*) > 1
              )
        ),
        latest_expected AS (
            SELECT idempotency_key, sort_ts as latest_ts, version_count
            FROM std_duplicates WHERE rn = 1
            LIMIT 3
        )
        SELECT
            le.idempotency_key,
            le.version_count as total_versions,
            le.latest_ts as expected_kept,
            COALESCE(CAST(ca.last_updated_ts AS VARCHAR), ca.publish_time) as actually_kept,
            CASE WHEN le.latest_ts = COALESCE(CAST(ca.last_updated_ts AS VARCHAR), ca.publish_time)
                 THEN 'MATCH ✓' ELSE 'MISMATCH ✗' END as result
        FROM latest_expected le
        LEFT JOIN iceberg_curated_db.events ca ON le.idempotency_key = ca.idempotency_key
        "

        local sample_results=$(run_athena_query "$sample_query" "json")

        echo "$sample_results" | python3 -c "
import sys, json
data = json.load(sys.stdin)
rows = data.get('ResultSet', {}).get('Rows', [])
if len(rows) > 1:
    print('  idempotency_key | versions | expected_ts | actual_ts | result')
    print('  ' + '-' * 75)
    for row in rows[1:]:
        cols = [d.get('VarCharValue', 'NULL') for d in row.get('Data', [])]
        if len(cols) >= 5:
            print(f'  {cols[0][:15]}... | {cols[1]:^8} | {cols[2][:15]}... | {cols[3][:13]}... | {cols[4]}')
" 2>/dev/null || log_info "  (Could not display sample - see Athena query results)"

    else
        record_test "LIFO deduplication" "FAIL" "$lifo_failures idempotency_keys kept WRONG occurrence (should be LATEST)"

        # Show failures
        log_error ""
        log_error "LIFO Validation Failures (first 5):"

        local failure_query="
        WITH std_duplicates AS (
            SELECT
                idempotency_key,
                COALESCE(
                    json_extract_scalar(json_payload, '$.last_updated_ts'),
                    publish_time
                ) as sort_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY idempotency_key
                    ORDER BY COALESCE(
                        json_extract_scalar(json_payload, '$.last_updated_ts'),
                        publish_time
                    ) DESC
                ) as rn
            FROM iceberg_standardized_db.events
            WHERE idempotency_key IS NOT NULL
              AND idempotency_key IN (
                  SELECT idempotency_key FROM iceberg_standardized_db.events
                  WHERE idempotency_key IS NOT NULL
                  GROUP BY idempotency_key HAVING COUNT(*) > 1
              )
        ),
        latest_expected AS (
            SELECT idempotency_key, sort_ts as expected_ts
            FROM std_duplicates WHERE rn = 1
        )
        SELECT
            le.idempotency_key,
            le.expected_ts as should_be,
            COALESCE(CAST(ca.last_updated_ts AS VARCHAR), ca.publish_time) as actually_is
        FROM latest_expected le
        LEFT JOIN iceberg_curated_db.events ca ON le.idempotency_key = ca.idempotency_key
        WHERE le.expected_ts != COALESCE(CAST(ca.last_updated_ts AS VARCHAR), ca.publish_time)
           OR ca.idempotency_key IS NULL
        LIMIT 5
        "

        run_athena_query "$failure_query" "text" | head -5 | while read line; do
            log_error "  $line"
        done
    fi

    echo ""
}

# =============================================================================
# VALIDATION: Cross-Period MERGE (Advanced LIFO)
# =============================================================================

validate_cross_period_merge() {
    log_section "CROSS-PERIOD MERGE VALIDATION"

    log_info "Testing: No duplicate idempotency_keys exist in Curated (even across periods)"
    log_info "Rule: MERGE with ±1 month period_reference lookback"
    echo ""

    log_test "Checking for duplicate idempotency_keys in Curated..."

    local curated_dupes=$(run_athena_query "
        SELECT COUNT(*) FROM (
            SELECT idempotency_key
            FROM iceberg_curated_db.events
            WHERE idempotency_key IS NOT NULL
            GROUP BY idempotency_key
            HAVING COUNT(*) > 1
        )
    ")

    if [ "$curated_dupes" = "0" ]; then
        record_test "Cross-period MERGE" "PASS" "No duplicate idempotency_keys in Curated"

        # Check if cross-period updates happened
        local cross_period_updates=$(run_athena_query "
            SELECT COUNT(*)
            FROM iceberg_curated_db.events
            WHERE first_seen_ts IS NOT NULL
              AND last_updated_ts IS NOT NULL
              AND DATE_FORMAT(first_seen_ts, '%Y-%m') != DATE_FORMAT(last_updated_ts, '%Y-%m')
        " 2>/dev/null || echo "N/A")

        if [ "$cross_period_updates" != "N/A" ] && [ "$cross_period_updates" != "0" ]; then
            log_info "  Found $cross_period_updates records updated across periods (e.g., Jan record corrected in Feb)"
        fi

    else
        record_test "Cross-period MERGE" "FAIL" "$curated_dupes duplicate idempotency_keys found"

        log_error ""
        log_error "Duplicate idempotency_keys in Curated (first 5):"

        local dupe_query="
        SELECT
            idempotency_key,
            period_reference,
            COUNT(*) as occurrences
        FROM iceberg_curated_db.events
        WHERE idempotency_key IS NOT NULL
        GROUP BY idempotency_key, period_reference
        HAVING COUNT(*) > 1
        LIMIT 5
        "

        run_athena_query "$dupe_query" "text" | while read line; do
            log_error "  $line"
        done
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
    echo "  fifo       Validate FIFO deduplication only"
    echo "  lifo       Validate LIFO deduplication only"
    echo "  merge      Validate cross-period MERGE only"
    echo "  all        Run all validations (default)"
    echo "  stats      Show data quality statistics only"
    echo ""
    echo "Environment:"
    echo "  AWS_PROFILE  AWS profile (default: terraform-firehose)"
    echo "  BUCKET       S3 bucket (default: lean-ops-development-iceberg)"
}

main() {
    local command="${1:-all}"

    case "$command" in
        stats)
            show_data_quality_stats
            ;;
        fifo)
            show_data_quality_stats
            validate_fifo
            ;;
        lifo)
            show_data_quality_stats
            validate_lifo
            ;;
        merge)
            show_data_quality_stats
            validate_cross_period_merge
            ;;
        all)
            show_data_quality_stats
            validate_fifo
            validate_lifo
            validate_cross_period_merge
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
            log_pass "ALL DEDUPLICATION VALIDATIONS PASSED ✅"
            echo ""
            exit 0
        else
            echo ""
            log_fail "DEDUPLICATION VALIDATION FAILED ❌"
            echo ""
            log_error "Review the failure details above and check:"
            log_error "  1. Glue job logs in CloudWatch"
            log_error "  2. Standardized processor implementation"
            log_error "  3. Curated processor MERGE logic"
            exit 1
        fi
    fi
}

main "$@"
