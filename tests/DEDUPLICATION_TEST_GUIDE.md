# Deduplication Testing Guide

**Purpose:** Validate FIFO and LIFO deduplication with clear, actionable test results

**Status:** ✅ Enhanced validation scripts added with detailed logging

---

## Quick Start

### Option 1: Run Full E2E Test with Validation

```bash
# Run complete test suite (includes deduplication validation)
./scripts/run_tests.sh e2e

# After tests complete, run explicit deduplication validation
./tests/validation/deduplication_validation.sh all
```

### Option 2: Phased Approach (Your Plan)

```bash
# Step 1: Deploy infrastructure
./scripts/run_tests.sh deploy

# Step 2: Run Day 1 (RAW only - no schema)
./tests/e2e/production_emulation.sh day1

# Step 3: Upload schema (enables processing)
./tests/e2e/production_emulation.sh schema

# Step 4: EventBridge triggers automatically OR manually trigger
./tests/e2e/production_emulation.sh trigger

# Step 5: Run Day 2 (with schema - includes duplicates for testing)
./tests/e2e/production_emulation.sh day2

# Step 6: Validate deduplication
./tests/validation/deduplication_validation.sh all
```

---

## What the Validation Script Does

### 1. Data Quality Statistics

Shows comprehensive pipeline stats:
```
┌─────────────────────────────────────────────┐
│         DATA PIPELINE STATISTICS           │
├─────────────────────────────────────────────┤
│ RAW (events_staging):              100,000  │
│ Standardized (events):              99,500  │
│ Curated (events):                   95,000  │
│ Parse Errors:                          500  │
│ CDE Errors:                          4,500  │
└─────────────────────────────────────────────┘
```

### 2. FIFO Validation (Network Duplicates)

**Tests:** When duplicate `message_id` exists, FIRST occurrence is kept

**Output Example:**
```
========== FIFO VALIDATION (Network Deduplication) ==========

[TEST] Step 1: Checking for duplicate message_ids in RAW...
[INFO] Found 150 message_ids with duplicates in RAW

[TEST] Step 2: Validating that FIRST occurrence was kept in Standardized...
[PASS] ✅ FIFO deduplication
  All 150 duplicate message_ids kept FIRST occurrence

Sample verification (first 3 duplicates):
  message_id | occurrences | expected_ts | actual_ts | result
  ----------------------------------------------------------------------
  msg-abc123... |      3      |  1000123456 | 1000123456 | MATCH ✓
  msg-def456... |      5      |  1000234567 | 1000234567 | MATCH ✓
  msg-ghi789... |      2      |  1000345678 | 1000345678 | MATCH ✓
```

**On Failure:**
```
[FAIL] ❌ FIFO deduplication
  5 message_ids kept WRONG occurrence (should be FIRST)

FIFO Validation Failures (first 5):
  msg-xxx: should_be=1000123456, actually_is=1000789012
  msg-yyy: should_be=1000234567, actually_is=1000890123
  ...
```

### 3. LIFO Validation (Business Corrections)

**Tests:** When duplicate `idempotency_key` exists, LATEST occurrence is kept

**Output Example:**
```
========== LIFO VALIDATION (Business Corrections) ==========

[TEST] Step 1: Checking for duplicate idempotency_keys in Standardized...
[INFO] Found 100 idempotency_keys with duplicates in Standardized

[TEST] Step 2: Validating that LATEST occurrence was kept in Curated...
[PASS] ✅ LIFO deduplication
  All 100 duplicate idempotency_keys kept LATEST occurrence

Sample verification (first 3 corrections):
  idempotency_key | versions | expected_ts | actual_ts | result
  ---------------------------------------------------------------------------
  order-123456... |    3     | 2026-01-01T... | 2026-01-01T... | MATCH ✓
  order-789012... |    2     | 2026-01-02T... | 2026-01-02T... | MATCH ✓
  ...
```

### 4. Cross-Period MERGE Validation

**Tests:** No duplicate `idempotency_key` in Curated (validates MERGE works across periods)

**Output Example:**
```
========== CROSS-PERIOD MERGE VALIDATION ==========

[TEST] Checking for duplicate idempotency_keys in Curated...
[PASS] ✅ Cross-period MERGE
  No duplicate idempotency_keys in Curated
  Found 23 records updated across periods (e.g., Jan record corrected in Feb)
```

---

## Detailed Test Scenarios

### Scenario 1: Network Duplicates (FIFO)

**Config:** `tests/configs/test_02_network_duplicates.json`

**What it does:**
- Generates 500 records
- 30% (150) have duplicate `message_id`
- Each duplicated up to 5 times with increasing `ingestion_ts`

**Expected behavior:**
1. RAW: 500 + (150 × avg 3 duplicates) = ~950 records
2. Standardized: 500 records (duplicates removed by FIFO)
3. For each duplicate group: Record with **earliest** `ingestion_ts` is kept

**Validation query:**
```sql
-- Verify FIRST was kept
WITH raw_duplicates AS (
    SELECT message_id, ingestion_ts,
           ROW_NUMBER() OVER (PARTITION BY message_id ORDER BY ingestion_ts ASC) as rn
    FROM iceberg_raw_db.events_staging
    WHERE message_id IN (
        SELECT message_id FROM iceberg_raw_db.events_staging
        GROUP BY message_id HAVING COUNT(*) > 1
    )
)
SELECT COUNT(*) as failures
FROM raw_duplicates rd
JOIN iceberg_standardized_db.events std ON rd.message_id = std.message_id
WHERE rd.rn = 1  -- Should be kept
  AND rd.ingestion_ts != std.ingestion_ts  -- But timestamps don't match!
-- Expected: 0 failures
```

**How to run:**
```bash
# Inject duplicate data
cd Testing_Framework_EEH_2.0/sample-data-generator
source .venv/bin/activate
python -m data_injector.main --config /Users/sagarm/Documents/Projects/Huntington_Exercises/lean-ops/tests/configs/test_02_network_duplicates.json

# Wait for Firehose buffer (90s)
sleep 90

# Trigger processing
cd /Users/sagarm/Documents/Projects/Huntington_Exercises/lean-ops
./tests/e2e/production_emulation.sh trigger

# Wait for Step Function completion (~5-10 min)

# Validate FIFO
./tests/validation/deduplication_validation.sh fifo
```

### Scenario 2: Business Corrections (LIFO)

**Config:** `tests/configs/test_03_business_corrections.json`

**What it does:**
- Generates 500 records
- 20% (100) have duplicate `idempotency_key`
- Each duplicated up to 3 times with **different payloads** and increasing `last_updated_ts`

**Expected behavior:**
1. RAW: 500 + (100 × avg 2 duplicates) = ~700 records
2. Standardized: ~700 records (FIFO removes message_id duplicates, not idempotency_key)
3. Curated: 500 records (LIFO keeps latest per idempotency_key)
4. For each duplicate group: Record with **latest** `last_updated_ts` is kept

**Validation query:**
```sql
-- Verify LATEST was kept
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
    WHERE idempotency_key IN (
        SELECT idempotency_key FROM iceberg_standardized_db.events
        GROUP BY idempotency_key HAVING COUNT(*) > 1
    )
)
SELECT COUNT(*) as failures
FROM std_duplicates sd
JOIN iceberg_curated_db.events cur ON sd.idempotency_key = cur.idempotency_key
WHERE sd.rn = 1  -- Should be kept (latest)
  AND sd.sort_ts != COALESCE(CAST(cur.last_updated_ts AS VARCHAR), cur.publish_time)
-- Expected: 0 failures
```

**How to run:**
```bash
# Inject correction data
python -m data_injector.main --config /Users/sagarm/Documents/Projects/Huntington_Exercises/lean-ops/tests/configs/test_03_business_corrections.json

# Wait, trigger, wait...

# Validate LIFO
./tests/validation/deduplication_validation.sh lifo
```

### Scenario 3: Cross-Period Corrections

**Config:** `tests/e2e/configs/day4_cross_period.json`

**What it does:**
- Generates 5,000 records spanning Jan/Feb/Mar 2026
- 30% are corrections of Day 1/Day 2 data (same `idempotency_key`, different `period_reference`)

**Expected behavior:**
1. MERGE uses `±1 month` lookback to find existing records
2. No duplicate `idempotency_key` in Curated (even across periods)
3. Corrections update existing records (not insert duplicates)

**Validation query:**
```sql
-- Check for duplicates (should be ZERO)
SELECT COUNT(*) FROM (
    SELECT idempotency_key, COUNT(*) as cnt
    FROM iceberg_curated_db.events
    GROUP BY idempotency_key
    HAVING COUNT(*) > 1
)
-- Expected: 0
```

**How to run:**
```bash
# Run Day 4 test
./tests/e2e/production_emulation.sh day4

# Validate cross-period MERGE
./tests/validation/deduplication_validation.sh merge
```

---

## Integration with Existing Tests

### Enhanced run_tests.sh

Add deduplication validation to E2E tests:

```bash
# After line 469 in scripts/run_tests.sh
# Add this before the summary:

# ========== DEDUPLICATION VALIDATION ==========
log_step "Running Deduplication Validation"

if [ -f "$PROJECT_ROOT/tests/validation/deduplication_validation.sh" ]; then
    "$PROJECT_ROOT/tests/validation/deduplication_validation.sh" all
    dedup_exit_code=$?

    if [ $dedup_exit_code -eq 0 ]; then
        log_info "✅ Deduplication validation PASSED"
        ((passed++))
    else
        log_error "❌ Deduplication validation FAILED"
        ((failed++))
    fi
else
    log_warn "Deduplication validation script not found (skipping)"
fi
```

### Enhanced production_emulation.sh

Add to `verify_day4()` function:

```bash
# After line 637 in tests/e2e/production_emulation.sh
# Add explicit deduplication validation:

log_info "Running explicit deduplication validation..."
if [ -f "$SCRIPT_DIR/../validation/deduplication_validation.sh" ]; then
    "$SCRIPT_DIR/../validation/deduplication_validation.sh" all
    if [ $? -eq 0 ]; then
        log_info "✅ Deduplication validation PASSED"
        ((passed++))
    else
        log_error "❌ Deduplication validation FAILED"
        ((failed++))
    fi
fi
```

---

## Your Phased Test Workflow

Based on your plan, here's the recommended workflow:

### Phase 1: Infrastructure Setup
```bash
cd /Users/sagarm/Documents/Projects/Huntington_Exercises/lean-ops

# Deploy infrastructure (Terraform + Glue jobs)
./scripts/run_tests.sh deploy
```

**Expected:**
- Terraform creates: Lambda, Firehose, SQS, Step Functions, Glue jobs
- S3 buckets exist but no schema files uploaded yet
- Processing is disabled (schema gate blocks it)

### Phase 2: Day 1 - RAW Only (No Schema)
```bash
# Inject initial data (100K clean records)
./tests/e2e/production_emulation.sh day1
```

**What happens:**
1. Data injector sends 100K records to SQS
2. Lambda processes SQS → Firehose → S3 RAW table
3. After 90s wait, Step Function is triggered
4. Step Function checks schema → NOT FOUND → skips processing
5. Data sits in RAW, waiting

**Validation:**
```bash
# Check RAW has data
aws athena start-query-execution \
    --query-string "SELECT COUNT(*) FROM iceberg_raw_db.events_staging" \
    --work-group primary

# Expected: ~100,000
```

### Phase 3: Schema Deployment (Enable Processing)
```bash
# Upload schema files to S3
./tests/e2e/production_emulation.sh schema
```

**What happens:**
1. `events.json` uploaded → enables Standardized processing
2. `curated_schema.json` uploaded → enables Curated processing
3. Next Step Function run will process data

**Note:** EventBridge triggers every hour (dev) or 15 min (prod) automatically.

### Phase 4: Wait for EventBridge OR Manually Trigger
```bash
# Option A: Wait for EventBridge (1 hour in dev)
# Just wait... ☕

# Option B: Manually trigger immediately
./tests/e2e/production_emulation.sh trigger
```

**What happens:**
1. Step Function runs
2. CheckSchema → FOUND → proceed
3. Standardized job processes RAW → Standardized (FIFO dedup)
4. Curated job processes Standardized → Curated (LIFO dedup)

**Monitor:**
```bash
# Check Step Function status
aws stepfunctions list-executions \
    --state-machine-arn $(terraform output -raw state_machine_arn) \
    --max-results 1

# Check Glue job runs
aws glue get-job-runs \
    --job-name lean-ops-dev-standardized-processor \
    --max-results 1
```

### Phase 5: Day 2 - Mixed Batch (With Duplicates)
```bash
# Inject corrections, duplicates, and errors
./tests/e2e/production_emulation.sh day2
```

**What happens:**
1. Injects 50K records with:
   - Network duplicates (30% - tests FIFO)
   - Business corrections (20% - tests LIFO)
   - Schema drift (new columns)
   - CDE violations (null required fields)
2. Wait 90s → Trigger Step Function
3. Processing runs with deduplication

### Phase 6: Validate Deduplication
```bash
# Run comprehensive validation
./tests/validation/deduplication_validation.sh all
```

**Expected output:**
```
========== DATA PIPELINE STATISTICS ==========
RAW: 155,000 records
Standardized: 145,000 records (10K network dupes removed)
Curated: 140,000 records (5K business corrections applied)

========== FIFO VALIDATION ==========
✅ PASS: All 4,500 duplicate message_ids kept FIRST occurrence

========== LIFO VALIDATION ==========
✅ PASS: All 2,500 duplicate idempotency_keys kept LATEST occurrence

========== CROSS-PERIOD MERGE VALIDATION ==========
✅ PASS: No duplicate idempotency_keys in Curated

========== VALIDATION SUMMARY ==========
Total tests: 3
Passed: 3
Failed: 0

✅ ALL DEDUPLICATION VALIDATIONS PASSED ✅
```

---

## Troubleshooting

### Issue: "No duplicates found - test SKIPPED"

**Cause:** Test data doesn't have duplicates

**Solution:**
```bash
# Check test configs have duplicates enabled
cat tests/configs/test_02_network_duplicates.json | grep -A 5 "duplicates"

# Should show:
# "duplicates": {
#     "enabled": true,
#     "percentage": 0.3,
#     ...
# }
```

### Issue: "FIFO validation FAILED"

**Cause:** Implementation kept wrong occurrence (LAST instead of FIRST)

**Debug:**
```bash
# Check Glue job logs
aws logs tail /aws-glue/jobs/lean-ops-dev-standardized-processor --follow

# Look for:
# "Stage 1 (FIFO): X -> Y records (Z removed)"

# Check implementation
cat scripts/glue/standardized_processor.py | grep -A 5 "window_fifo"

# Should see:
# .orderBy(F.col("ingestion_ts").asc())  # ASC = FIFO (first)
```

### Issue: "LIFO validation FAILED"

**Cause:** MERGE condition wrong or timestamp extraction failed

**Debug:**
```bash
# Check Curated processor logs
aws logs tail /aws-glue/jobs/lean-ops-dev-curated-processor --follow

# Check MERGE condition
cat scripts/glue/curated_processor.py | grep -A 3 "WHEN MATCHED"

# Should see:
# WHEN MATCHED AND s.last_updated_ts > t.last_updated_ts THEN
#                                      ^ Must be > not <
```

### Issue: "Cross-period MERGE shows duplicates"

**Cause:** MERGE lookback not working (±1 month logic)

**Debug:**
```bash
# Check MERGE ON clause
cat scripts/glue/curated_processor.py | grep -A 7 "MERGE INTO"

# Should see:
# ON t.idempotency_key = s.idempotency_key
#    AND (
#        t.period_reference = s.period_reference
#        OR t.period_reference = date_format(add_months(...), -1)  # -1 month
#        OR t.period_reference = date_format(add_months(...), 1)   # +1 month
#    )
```

---

## Command Reference

### Run All Validations
```bash
./tests/validation/deduplication_validation.sh all
```

### Run Individual Validations
```bash
# FIFO only
./tests/validation/deduplication_validation.sh fifo

# LIFO only
./tests/validation/deduplication_validation.sh lifo

# Cross-period MERGE only
./tests/validation/deduplication_validation.sh merge

# Statistics only (no validation)
./tests/validation/deduplication_validation.sh stats
```

### Quick Test Commands
```bash
# Full E2E with validation
./scripts/run_tests.sh e2e && ./tests/validation/deduplication_validation.sh all

# Phased approach (your workflow)
./scripts/run_tests.sh deploy && \
./tests/e2e/production_emulation.sh day1 && \
./tests/e2e/production_emulation.sh schema && \
./tests/e2e/production_emulation.sh trigger && \
./tests/e2e/production_emulation.sh day2 && \
./tests/validation/deduplication_validation.sh all
```

---

## Success Criteria

✅ **Production Ready** when ALL of these pass:

1. ✅ FIFO validation: 0 failures
2. ✅ LIFO validation: 0 failures
3. ✅ Cross-period MERGE: 0 duplicates in Curated
4. ✅ Data accountability: RAW count = Standardized + Parse Errors
5. ✅ Data accountability: Standardized count = Curated + CDE Errors

**Current Status:** ⚠️ Tests exist, need execution to confirm

---

## Next Steps

1. **Run the tests** using the phased approach above
2. **Document results** - save validation output to `tests/results/`
3. **Fix any failures** - use debug queries in this guide
4. **Re-run until green** - all validations must pass
5. **Add to CI/CD** - automate in GitHub Actions

**Estimated Time:** 2-3 hours for full test cycle
