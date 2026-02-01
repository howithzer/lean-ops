# Lessons Learned: Iceberg Curated Layer

## Summary of Issues Encountered

| Issue | Root Cause | Solution |
|-------|------------|----------|
| MERGE not working | Missing `IcebergSparkSessionExtensions` | Add to `--conf` in Glue job |
| Table not found | Missing `glue_catalog.` prefix | Use `glue_catalog.db.table` syntax |
| Partitions lost | `writeTo().createOrReplace()` replaced DDL table | Use append mode after initial load |
| Table properties lost | Same as above | Set properties after table has data |

---

## Recommended Approach: Start Fresh

### 1. Table Creation Strategy

**Problem:** `writeTo().createOrReplace()` creates a new table with default settings, losing any DDL-defined partitioning and properties.

**Recommendation:** Two-phase approach:

```
Phase 1: Create empty table with proper DDL (Athena/Terraform)
         ↓
Phase 2: Glue job uses INSERT INTO or MERGE (never createOrReplace)
```

### 2. Curated Table DDL (Recommended)

```sql
CREATE TABLE IF NOT EXISTS iceberg_curated_db.events (
  -- Envelope fields (static)
  message_id        STRING     COMMENT 'Unique message ID for FIFO dedup',
  idempotency_key   STRING     COMMENT 'Business key for LIFO dedup',
  topic_name        STRING     COMMENT 'Source topic',
  period_reference  STRING     COMMENT 'Business period (e.g., 2026-01)',
  correlation_id    STRING     COMMENT 'Trace correlation',
  publish_time      STRING     COMMENT 'Source publish timestamp',
  ingestion_ts      BIGINT     COMMENT 'RAW ingestion timestamp (epoch)',
  
  -- Payload fields (auto-evolve via ALTER TABLE ADD COLUMNS)
  device_id         STRING,
  event_type        STRING,
  event_timestamp   STRING,
  sensor_reading    STRING,
  location          STRING,
  status            STRING,
  amount            STRING,
  currency          STRING,
  user_id           STRING
)
PARTITIONED BY (
  period_reference,
  day(publish_time)
)
LOCATION 's3://lean-ops-development-iceberg/iceberg_curated_db/events/'
TBLPROPERTIES (
  -- Required
  'table_type' = 'ICEBERG',
  'format' = 'parquet',
  
  -- File optimization
  'write.target-file-size-bytes' = '268435456',     -- 256MB
  'write.distribution-mode' = 'range',
  'write_compression' = 'zstd',
  
  -- Manifest optimization
  'commit.manifest.target-size-bytes' = '8388608', -- 8MB
  'commit.manifest.min-count-to-merge' = '50',
  
  -- Snapshot retention (7 days)
  'history.expire.max-snapshot-age-ms' = '604800000',
  'history.expire.min-snapshots-to-keep' = '50'
);
```

> **CRITICAL:** Athena partitioning for Iceberg works ONLY at table creation time!
>
> ✅ **Works** (define partitions in CREATE TABLE):
> ```sql
> CREATE TABLE events (...) 
> PARTITIONED BY (period_reference, day(publish_time))
> ```
>
> ❌ **Fails** (add partitions after creation):
> ```sql
> ALTER TABLE events ADD PARTITION FIELD period_reference  -- Error!
> ```
>
> ✅ **Alternative** (use Spark SQL in Glue jobs):
> ```python
> spark.sql("ALTER TABLE glue_catalog.db.events ADD PARTITION FIELD period_reference")
> ```
>
> **Docs**: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html

### 3. Glue Job Configuration

```hcl
# Terraform: modules/orchestration/main.tf
default_arguments = {
  "--job-language"                     = "python"
  "--job-bookmark-option"              = "job-bookmark-disable"
  "--enable-continuous-cloudwatch-log" = "true"
  "--enable-spark-ui"                  = "true"
  "--datalake-formats"                 = "iceberg"
  
  # CRITICAL: Enable MERGE/UPDATE/DELETE
  "--conf" = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
}
```

### 4. Glue Script Pattern

```python
# curated_processor.py

# Configure Iceberg catalog (REQUIRED)
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

# Use glue_catalog prefix for all table access
RAW_TABLE = f"glue_catalog.{RAW_DATABASE}.{TOPIC_NAME}_staging"
CURATED_TABLE = f"glue_catalog.{CURATED_DATABASE}.events"

# For first run with EMPTY table: Use INSERT INTO, not writeTo().createOrReplace()
# MERGE works on empty tables too!
```

### 5. First-Run Pattern (Fixed)

**Instead of:**
```python
# ❌ This replaces the table, losing DDL settings
df.writeTo(CURATED_TABLE).using("iceberg").createOrReplace()
```

**Use:**
```python
# ✅ This preserves DDL partitioning and properties
merge_sql = f"""
MERGE INTO {CURATED_TABLE} t
USING staged_data s
ON t.idempotency_key = s.idempotency_key
WHEN MATCHED AND s.publish_time > t.publish_time THEN
    UPDATE SET ...
WHEN NOT MATCHED THEN
    INSERT (...)
"""
spark.sql(merge_sql)
```

MERGE works even on empty tables when `IcebergSparkSessionExtensions` is enabled!

---

## Recommended Table Properties for Curated Layer

| Property | Value | Purpose |
|----------|-------|---------|
| `format` | parquet | Standard, Athena-compatible |
| `write_compression` | zstd | Best compression ratio |
| `write.target-file-size-bytes` | 268435456 | 256MB files (optimal for Athena) |
| `write.distribution-mode` | range | Better file layout |
| `commit.manifest.target-size-bytes` | 8388608 | 8MB manifests |
| `commit.manifest.min-count-to-merge` | 50 | Auto-consolidate manifests |
| `history.expire.max-snapshot-age-ms` | 604800000 | 7-day snapshot retention |

---

## Recommended Partitioning Strategy

| Field | Transform | Rationale |
|-------|-----------|-----------|
| `period_reference` | identity | Business period alignment (monthly reporting) |
| `day(publish_time)` | day | Time-based queries (last N days) |

**Query Benefit:**
```sql
-- This query prunes to specific partitions
SELECT * FROM events 
WHERE period_reference = '2026-01' 
  AND publish_time >= '2026-01-10'
```

---

## IAM Role Requirements

```hcl
# Required policies for Glue role
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
```

Don't forget custom S3, Glue Catalog, and DynamoDB permissions!

---

## Maintenance Operations

### A. Snapshot Expiration (Weekly)
```python
spark.sql(f"""
  CALL glue_catalog.system.expire_snapshots(
    table => '{CURATED_TABLE}',
    older_than => TIMESTAMP '...',
    retain_last => 50
  )
""")
```

### B. Orphan File Cleanup (Weekly)
```python
spark.sql(f"""
  CALL glue_catalog.system.remove_orphan_files(
    table => '{CURATED_TABLE}',
    older_than => TIMESTAMP '...'
  )
""")
```

### C. Manifest Rewrite (After Expiration)
```python
spark.sql(f"CALL glue_catalog.system.rewrite_manifests(table => '{CURATED_TABLE}')")
```

---

## Summary: Key Takeaways

1. **Always use `glue_catalog.db.table`** syntax in Spark SQL
2. **Enable `IcebergSparkSessionExtensions`** for MERGE/UPDATE/DELETE
3. **Create table via DDL first**, then use MERGE (not createOrReplace)
4. **Add partitioning via ALTER TABLE** (Athena DDL limitation)
5. **Set table properties** for file/manifest optimization
6. **Attach `AWSGlueServiceRole`** managed policy
7. **Schedule maintenance** (snapshot expiration, orphan cleanup)

---

## 3. Debugging CDE Validation (The "Zero Errors" Mystery)

### Context
We enabled error injection (5% empty payloads, 3% CDE violations) but initially saw **0 CDE errors** in the `errors` table, despite correct configuration.

### The 3 Hidden Bugs

#### Bug A: Pydantic Config Structure Mismatch (Silent Failure)
**Issue:** `day2_mixed_batch.json` had `error_injection` at the top level, but the Pydantic model expected it inside `schema_config`.
**Result:** Pydantic silently ignored the top-level key and used defaults (`enabled=False`), so valid data was generated instead of errors.
**Fix:** Moved `error_injection` block inside `schema_config`.

#### Bug B: SQS Processor "Safety" Fallback (Data Loss)
**Issue:** The Lambda logic tried to be "helpful" by falling back to `message_id` if `idempotency_key` was null.
```python
idempotency_key = idempotency_key or message_id  # ❌ Replaced intentional NULLs
```
**Result:** NULL values injected by the generator never reached the pipeline; they were "fixed" at ingestion.
**Fix:** Removed the fallback to allow NULL values to propagate for validation.

#### Bug C: Errors Table Schema Mismatch (Write Failure)
**Issue:** The validation logic worked (logs showed 1,448 violations detected), but writing to `iceberg_curated_db.errors` failed silently (non-fatal error).
**Detail:** The code tried to write 7 columns (`message_id`, `raw_record`, etc.), but the table DDL only defined 4.
**Fix:** Updated `tests/e2e/production_emulation.sh` to add the missing columns to the CREATE TABLE statement.

### Key Lesson
**"Zero Errors" allows false confidence.** Always verify negative tests by seeing the specific errors count up in the target table. If you inject 3% errors and see 0%, assume the injection failed or the pipeline is masking them, not that the data is "perfect".
