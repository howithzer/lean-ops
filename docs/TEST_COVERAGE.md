# Test Coverage Matrix

## Current Test Files

| File | Type | Scope | Status |
|------|------|-------|--------|
| `tests/test_common.py` | Unit | Common library (topic_utils, error_classification) | ✅ |
| `tests/unit/test_schema_registry_phase2.sh` | Unit | Schema registration workflow | ✅ |
| `tests/e2e/production_emulation.sh` | E2E | Full pipeline (Day 1→Schema→Day 2) | ✅ |
| `tests/validation/` | Validation | Athena query checks | ✅ |

---

## Test Scenarios

### Happy Path Tests

| Scenario | Test | Expected | Covered |
|----------|------|----------|---------|
| Initial data load | E2E Day 1 | RAW + Standardized populated | ✅ |
| Schema deployment | E2E schema phase | Schema in active/, DynamoDB flag=true | ✅ |
| Data processing | E2E Day 2 | Curated table populated | ✅ |
| FIFO deduplication | E2E replay | Network duplicates removed | ✅ |
| LIFO deduplication | E2E corrections | Business corrections applied | ✅ |
| Schema evolution | E2E drift | New columns auto-added | ✅ |

### Negative/Error Tests

| Scenario | Test | Expected | Covered |
|----------|------|----------|---------|
| No schema | E2E Day 1 | Step Function skips Curated | ✅ |
| Invalid JSON | E2E empty_payload | → parse_errors table | ✅ |
| CDE violation | E2E cde_violation | → errors table | ✅ |
| Invalid schema file | Unit test | → failed/ folder + error.json | ✅ |
| Missing required fields | Unit test | Validation fails | ✅ |

### Edge Cases

| Scenario | Test | Expected | Covered |
|----------|------|----------|---------|
| Empty flow | E2E after catchup | "0 records" → exits cleanly | ⚠️ Manual |
| Late arrivals | E2E late_arrival | Older records don't overwrite | ✅ |
| Type drift | E2E drift_type | Logged, handled gracefully | ✅ |
| Column removal | E2E drift_missing | Null values inserted | ✅ |

---

## Test Commands

```bash
# Unit tests
pytest tests/test_common.py -v
./tests/unit/test_schema_registry_phase2.sh

# E2E tests (requires deployed infra)
./tests/e2e/production_emulation.sh clean
./tests/e2e/production_emulation.sh day1
./tests/e2e/production_emulation.sh schema
./tests/e2e/production_emulation.sh day2
./tests/e2e/production_emulation.sh verify

# Or all at once
./tests/e2e/production_emulation.sh full
```

---

## Future Test Additions (Phase 3+)

| Scenario | Priority |
|----------|----------|
| Table DDL creation from schema | High |
| Drift backfill job | Medium |
| Maintenance mode toggle | Medium |
| REBUILD command | Future |
| DLQ replay | Future |
