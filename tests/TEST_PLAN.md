# Lean-Ops: Test Plan

## Test Suite Overview

| Test | Scenario | Curated Expectation | Semantic Expectation |
|------|----------|---------------------|----------------------|
| 01 | Happy Path | ✅ All records | ✅ All records |
| 02 | Network Duplicates | ✅ FIFO dedup | ✅ FIFO dedup |
| 03 | Business Corrections | ✅ LIFO dedup | ✅ LIFO dedup |
| 04 | Drift: Add Columns | ✅ Auto-add cols | ✅ Log drift only |
| 05 | Drift: Type Changes | ✅ All STRING | ❌ May fail on cast |
| 06 | CDE Violations | ✅ All records | ⚠️ Errors to table |
| 07 | Combined | ✅ All processed | ✅ Schema cols only |

---

## Test Execution

### Prerequisites

1. **Data Generator** is at:
   ```
   /Users/sagarm/Documents/Projects/Huntington_Exercises/Testing_Framework_EEH_2.0/sample-data-generator
   ```

2. **Activate venv**:
   ```bash
   cd /Users/sagarm/Documents/Projects/Huntington_Exercises/Testing_Framework_EEH_2.0/sample-data-generator
   source .venv/bin/activate
   ```

### Generate Test Data

```bash
# Test 01: Happy Path
python -m data_injector.main --config ../../lean-ops/tests/configs/test_01_happy_path.json

# Test 02: Network Duplicates
python -m data_injector.main --config ../../lean-ops/tests/configs/test_02_network_duplicates.json

# Test 03: Business Corrections
python -m data_injector.main --config ../../lean-ops/tests/configs/test_03_business_corrections.json

# ... etc
```

---

## Test Case Details

### Test 01: Happy Path

**Input**: 500 records, no duplicates, no drift

**Assertions**:
- RAW count = 500
- Curated count = 500
- Semantic count = 500
- No errors in errors table

---

### Test 02: Network Duplicates (FIFO)

**Input**: 500 records, 30% duplicates on `messageId`

**Assertions**:
- RAW count = ~650 (includes dupes)
- Curated count = 500 (dupes removed, FIRST wins)
- Semantic count = 500
- Same `message_id` → same `ingestion_ts` (first one kept)

---

### Test 03: Business Corrections (LIFO)

**Input**: 500 records, 20% corrections on `idempotencyKeyResource`

**Assertions**:
- After FIFO dedup: ~500
- After LIFO dedup: ~400 (corrections applied)
- Same `idempotency_key` → LAST `ingestion_ts` kept
- Payload reflects latest correction

---

### Test 04: Schema Drift - Add Columns

**Input**: 500 records, 30% with new columns

**Assertions**:
- **Curated**: New columns added via DDL
- **Semantic**: Drift logged to `drift_log`, no new columns
- Both succeed

---

### Test 05: Schema Drift - Type Changes

**Input**: 500 records, 50% with type changes (int→string, etc.)

**Assertions**:
- **Curated**: ✅ Success (all STRING, no type issues)
- **Semantic**: ❌ May fail on CAST errors
- Tiered outcome: `CURATED_ONLY`

---

### Test 06: CDE Violations

**Input**: 500 records, 20% missing required CDEs

**Assertions**:
- **Curated**: 500 records (CDEs not enforced)
- **Semantic**: ~400 valid, ~100 in errors table
- errors table has `error_type = 'CDE_VIOLATION'`

---

### Test 07: Combined Scenario

**Input**: 1000 records with duplicates + drift

**Assertions**:
- Dedup reduces count
- Curated has all columns (including new ones)
- Semantic has schema columns only
- Full success expected

---

## Validation Queries

```sql
-- RAW count
SELECT COUNT(*) FROM iceberg_raw_db.events_staging;

-- Curated count
SELECT COUNT(*) FROM curated_db.events;

-- Semantic count
SELECT COUNT(*) FROM semantic_db.events;

-- Errors count
SELECT COUNT(*) FROM semantic_db.errors WHERE topic_name = 'events';

-- Drift log
SELECT * FROM semantic_db.drift_log WHERE topic_name = 'events' ORDER BY detected_at DESC;

-- Check dedup worked (should be unique)
SELECT message_id, COUNT(*) 
FROM curated_db.events 
GROUP BY message_id 
HAVING COUNT(*) > 1;

-- Check LIFO (latest correction wins)
SELECT idempotency_key, MAX(curated_ts) 
FROM curated_db.events 
GROUP BY idempotency_key;
```

---

## Checkpoint Verification

```sql
-- Check independent checkpoints
SELECT * FROM lean_ops_checkpoints 
WHERE topic_name = 'events';

-- After Test 05 (type changes causing semantic failure):
-- curated_checkpoint = updated
-- semantic_checkpoint = NOT updated
```
