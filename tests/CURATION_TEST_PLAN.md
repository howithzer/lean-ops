# Curation Layer Testing Strategy

## Test Categories

| Category | Tests | Priority |
|----------|-------|----------|
| **Schema Gate Tests** | 6 | High |
| **Table Creation Tests** | 4 | High |
| **MERGE Operation Tests** | 5 | High |
| **Schema Drift Tests** | 4 | Medium |
| **Error Handling Tests** | 5 | High |
| **Checkpoint Tests** | 3 | Medium |

---

## 1. Schema Gate Tests

### Positive Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| SG-01 | Schema file exists in S3 | Step Function proceeds to EnsureCuratedTable |
| SG-02 | Schema file has valid JSON | Lambda parses successfully |

### Negative Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| SG-03 | Schema file NOT in S3 | Step Function → SkipNoSchema, graceful exit |
| SG-04 | Schema file is empty | EnsureCuratedTable fails, HandleError state |
| SG-05 | Schema file is invalid JSON | EnsureCuratedTable fails, HandleError state |
| SG-06 | Wrong S3 bucket permissions | Lambda fails with AccessDenied, retry |

### Validation Commands

```bash
# Check if schema exists
aws s3 ls s3://lean-ops-development-iceberg/schemas/events.json

# Test without schema (should skip)
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:...:lean-ops-dev-unified-orchestrator \
  --input '{"topic_name":"nonexistent_topic"}'
```

---

## 2. Table Creation Tests

### Positive Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| TC-01 | Table does not exist | Table created with proper DDL |
| TC-02 | Table already exists | Skipped, return {"status": "exists"} |

### Negative Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| TC-03 | Glue database does not exist | Lambda fails, Athena error |
| TC-04 | Invalid column names in schema | CREATE TABLE fails |
| TC-05 | S3 location already has data | Table creates successfully (Iceberg metadata written) |

### Validation Queries

```sql
-- Check if table exists
SHOW TABLES IN iceberg_curated_db;

-- Check table schema
DESCRIBE iceberg_curated_db.events;

-- Check table location
SHOW CREATE TABLE iceberg_curated_db.events;
```

---

## 3. MERGE Operation Tests

### Positive Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| MG-01 | First load (empty table) | All records inserted |
| MG-02 | Subsequent load (no matches) | All new records inserted |
| MG-03 | Updates (same idempotency_key, newer publish_time) | Records updated |

### Negative Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| MG-04 | Duplicate idempotency_key, older publish_time | No update (LIFO respected) |
| MG-05 | NULL idempotency_key | Record inserted (treated as unique) |
| MG-06 | IcebergSparkSessionExtensions missing | MERGE fails with "not supported" error |
| MG-07 | Column count mismatch | Glue job fails |

### Validation Queries

```sql
-- Check deduplication worked
SELECT idempotency_key, COUNT(*) 
FROM iceberg_curated_db.events 
GROUP BY idempotency_key 
HAVING COUNT(*) > 1;

-- Expected: 0 rows (no duplicates)

-- Check update worked (latest publish_time)
SELECT idempotency_key, publish_time 
FROM iceberg_curated_db.events 
WHERE idempotency_key IN (
  SELECT idempotency_key FROM staging_view
);
```

---

## 4. Schema Drift Tests

### Positive Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| SD-01 | New field in json_payload | Column added via ALTER TABLE |
| SD-02 | Field removed from payload | No error, column stays (NULL for new records) |

### Negative Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| SD-03 | New field with reserved name | ALTER TABLE fails |
| SD-04 | Field name with special characters | ALTER TABLE fails |

### Validation Queries

```sql
-- Check new columns were added
SHOW COLUMNS IN iceberg_curated_db.events;

-- Check data in new columns
SELECT new_column_name 
FROM iceberg_curated_db.events 
WHERE new_column_name IS NOT NULL 
LIMIT 10;
```

---

## 5. Error Handling Tests

### Negative Cases (Failure Modes)

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| EH-01 | Glue job timeout | Step Function → HandleError → SNS alert |
| EH-02 | Out of memory in Glue job | Job fails, Step Function catches |
| EH-03 | S3 bucket inaccessible | Glue job fails immediately |
| EH-04 | DynamoDB checkpoint write fails | Job completes but checkpoint not updated |
| EH-05 | Athena workgroup disabled | EnsureCuratedTable fails |

### Recovery Tests

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| REC-01 | Re-run after failure | Picks up from last checkpoint |
| REC-02 | Re-run after partial write | MERGE handles duplicates correctly |

---

## 6. Checkpoint Tests

### Positive Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| CP-01 | First run (no checkpoint) | Reads all RAW data |
| CP-02 | Subsequent run | Reads only data after checkpoint |

### Negative Cases

| Test ID | Scenario | Expected Result |
|---------|----------|-----------------|
| CP-03 | Checkpoint in future | Returns empty dataset |
| CP-04 | Checkpoint table missing | Treats as first run |

---

## E2E Test Sequence

### Happy Path (Full Flow)

```
1. Upload schema to S3 (if not present)
2. Run Step Function with topic_name="events"
3. Verify:
   - Schema exists → SchemaGate passes
   - Table created or exists
   - Glue job completes
   - Curated table has records
   - Checkpoint updated in DynamoDB
```

### Negative Path (No Schema)

```
1. Ensure schema file does NOT exist in S3
2. Run Step Function with topic_name="orders" (no schema)
3. Verify:
   - SchemaGate returns SkipNoSchema
   - No table creation attempted
   - Glue job NOT executed
   - Execution ends with status="skipped"
```

### Schema Drift Path

```
1. Run E2E with 500 records (normal schema)
2. Send 500 records with new column "extra_field"
3. Run Glue job
4. Verify:
   - ALTER TABLE ADD COLUMNS executed
   - New records have "extra_field" populated
   - Old records have NULL in "extra_field"
```

---

## Test Execution Commands

```bash
# Test 1: No schema -> skip
aws stepfunctions start-execution \
  --state-machine-arn $STATE_MACHINE_ARN \
  --input '{"topic_name":"no_schema_topic"}'

# Test 2: With schema -> table created
aws s3 cp schemas/events.json s3://lean-ops-development-iceberg/schemas/
aws stepfunctions start-execution \
  --state-machine-arn $STATE_MACHINE_ARN \
  --input '{"topic_name":"events"}'

# Test 3: Data with drift
python -m data_injector.main --config tests/configs/schema_drift_sqs.json
# Then run Step Function

# Verify table has new columns
aws athena start-query-execution \
  --query-string "DESCRIBE iceberg_curated_db.events" \
  --work-group primary
```
