# Test Framework: Production Emulation

## Philosophy

> **Emulate production, don't tick checkboxes**

Instead of running all tests in one synthetic batch, we run **phased operations** that mirror real production lifecycle.

---

## Edge Case Scenarios (Stage Gate Testing)

| Scenario | Test | Expected Behavior |
|----------|------|-------------------|
| **No Schema** | Day 1 before schema deploy | Step Function hits schema check → `CheckCuratedReady` returns `{exists: false}` → skips Curated |
| **Empty Flow** | Run after checkpoint caught up | Job checks snapshot offsets → "0 records to process" → exits cleanly |
| **Invalid Schema** | Deploy malformed JSON | Curated job `load_schema()` → try/catch → clear error message |
| **100% Accountability** | All scenarios | Every record in `success_table` OR `error_table` - nothing lost |

---

## Test Flow Overview

```
┌────────────────────────────────────────────────────────────────────┐
│ DAY 1: INITIAL LOAD (No Schema)                    ~15-20 min     │
├────────────────────────────────────────────────────────────────────┤
│ 1. Inject 100K "clean" records                                    │
│ 2. Wait for scheduled EventBridge (or trigger manually)           │
│ 3. Step Function: RAW → Standardized → Curated (no schema yet)    │
│ 4. Verify: RAW=100K, Standardized=100K, Curated waiting           │
│ 5. REPORT: "Day 1 Operations ✓"                                   │
└────────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────────┐
│ SCHEMA DEPLOYMENT                                                  │
├────────────────────────────────────────────────────────────────────┤
│ 1. Upload curated_schema.json to S3                               │
│ 2. Next scheduled run will pick it up                             │
└────────────────────────────────────────────────────────────────────┘
                              ↓
┌────────────────────────────────────────────────────────────────────┐
│ DAY 2: CORRECTIONS + DRIFT                         ~15-20 min     │
├────────────────────────────────────────────────────────────────────┤
│ 1. Inject mixed batch:                                            │
│    - Correction records (same key, newer timestamp)               │
│    - Late arrivals (same key, older timestamp)                    │
│    - Schema drift (new columns, missing columns, type changes)    │
│    - Error cases (empty payload, CDE violations)                  │
│ 2. Wait for scheduled EventBridge                                 │
│ 3. Step Function processes with schema                            │
│ 4. Verify: MERGE behavior, schema evolution, error routing        │
│ 5. REPORT: "Day 2 Operations ✓"                                   │
└────────────────────────────────────────────────────────────────────┘
```

---

## Data Generation Strategy

### Day 1: Initial Load (100K Records)
```json
{
  "phase": "day1",
  "scenarios": [
    {"test_id": "initial_load", "count": 100000, "description": "Clean baseline records"}
  ]
}
```

### Day 2: Mixed Batch (50K Records)
```json
{
  "phase": "day2",
  "scenarios": [
    {"test_id": "correction", "count": 5000, "merge": "newer_timestamp"},
    {"test_id": "late_arrival", "count": 5000, "merge": "older_timestamp"},
    {"test_id": "replay", "count": 5000, "merge": "exact_duplicate"},
    {"test_id": "drift_add_column", "count": 10000, "drift": {"add": ["verb"]}},
    {"test_id": "drift_missing", "count": 10000, "drift": {"delete": ["userId"]}},
    {"test_id": "drift_type", "count": 10000, "drift": {"type_change": {"amount": "string"}}},
    {"test_id": "empty_payload", "count": 3000, "payload": null},
    {"test_id": "cde_violation", "count": 2000, "cde_nulls": ["event_timestamp"]}
  ]
}
```

---

## Verification Queries

### Day 1 Verification
```sql
SELECT COUNT(*) FROM iceberg_raw_db.events_staging 
WHERE json_extract_scalar(json_payload, '$.test_id') = 'initial_load';
-- Expected: 100,000

SELECT COUNT(*) FROM iceberg_standardized_db.events 
WHERE test_id = 'initial_load';
-- Expected: 100,000
```

### Day 2 Verification

**MERGE Tests:**
```sql
-- Corrections: newer should win
SELECT COUNT(*) FROM iceberg_curated_db.events WHERE test_id = 'correction';
-- Expected: 5000 (not 10000)

-- Late arrivals: older loses
SELECT COUNT(*) FROM iceberg_curated_db.events WHERE test_id = 'late_arrival';
-- Expected: 5000

-- Replay: idempotent
SELECT COUNT(*) FROM iceberg_curated_db.events WHERE test_id = 'replay';
-- Expected: 5000 (not 10000)
```

**Error Routing:**
```sql
SELECT COUNT(*) FROM iceberg_standardized_db.parse_errors;
-- Expected: 3000 (empty payloads)

SELECT COUNT(*) FROM iceberg_curated_db.errors WHERE error_type = 'CDE_VIOLATION';
-- Expected: 2000
```

---

## Usage

```bash
# Step-by-step (manual verification between phases)
./tests/e2e/production_emulation.sh day1
# ... wait 15-20 min, review results ...
./tests/e2e/production_emulation.sh schema
./tests/e2e/production_emulation.sh day2
# ... wait 15-20 min, review results ...

# Or fully automated
./tests/e2e/production_emulation.sh full
```

---

## Expected Results

| Metric | Before | After |
|--------|--------|-------|
| Total Runtime | ~60 min | ~35 min |
| Realism | Synthetic | Production-like |
| EventBridge | Bypassed | Actually used |
| Schema Lifecycle | Pre-loaded | Deployed mid-test |
| MERGE Testing | Same batch | Cross-batch |
