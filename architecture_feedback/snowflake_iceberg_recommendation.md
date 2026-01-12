# Snowflake‑External‑Iceberg Integration Recommendation

## 1. How Snowflake sees the Semantic Iceberg table

| Snowflake object | What it stores | How it maps to your Iceberg table |
|------------------|----------------|-----------------------------------|
| **Database → Schema → Table** (type = `EXTERNAL ICEBERG`) | A pointer to the Iceberg **metadata location** (`s3://…/semantic/`) and a **catalog name** (`glue`). | Snowflake reads the same manifest files that your Iceberg engine uses, so the data is **single‑source**. |
| **External Stage** | S3 bucket + IAM role that Snowflake uses to read the files. | Must have read permission on the bucket that holds the Iceberg data files and the **metadata** folder. |
| **File Format** (optional) | Parquet (or ORC) – the format you already write. | No conversion needed; Snowflake can query Parquet directly. |

> **Key point:** Snowflake does **not** copy the data – it queries the files in place. Therefore any schema change that Iceberg records (new column, new partition) is instantly visible to Snowflake **once the Iceberg metadata is refreshed**.

---

## 2. Schema‑evolution handling for Snowflake

| Situation | What happens in Iceberg | What Snowflake needs | Recommended approach |
|-----------|--------------------------|----------------------|----------------------|
| **New column added in Curated** (auto‑evolve) | Iceberg adds a new `STRING` column to the table metadata. | Snowflake sees the column **only after** you run `ALTER EXTERNAL ICEBERG TABLE … REFRESH`. | *Automation*: create a Lambda (or Step‑Function) that triggers on a DynamoDB `drift_log` entry, runs `REFRESH` via Snowflake’s **Snowpipe‑API** or the **Snowflake REST endpoint**. |
| **Column promoted to Semantic** (strict) | Iceberg schema version is updated with a typed column (e.g., `DOUBLE`). | Snowflake must **re‑register the column type** – it reads the new type from the refreshed metadata. | After the promotion PR, add a post‑deploy step that runs `ALTER EXTERNAL ICEBERG TABLE semantic_events REFRESH;`. Snowflake will automatically map the Iceberg type to the Snowflake type (e.g., `DOUBLE` → `FLOAT`). |
| **Column removed / deprecated** | Iceberg creates a new snapshot without the column. | Snowflake will still show the column until a refresh; after refresh the column disappears. | Include a `DROP COLUMN` in the promotion workflow (Iceberg DDL) and then run `REFRESH`. |

**Why a refresh is required:** Snowflake caches the Iceberg schema in its catalog. A `REFRESH` forces Snowflake to read the latest Iceberg manifest list.

---

## 3. Performance‑tuning tips for Snowflake queries on the Semantic Iceberg table

| Tuning lever | Why it matters for Iceberg + Snowflake | Practical setting |
|--------------|----------------------------------------|-------------------|
| **Partition pruning** | Iceflake can prune files based on Iceberg partition columns (`period_reference`, `day(publish_time)`). | Keep the same partitioning strategy you already use for the Semantic layer (see `layer_definitions.md`). Snowflake will automatically push predicates to the partition filter. |
| **Clustering keys** (optional) | If you have very high cardinality columns that are frequently filtered (e.g., `device_id`), clustering can improve micro‑partition pruning inside Snowflake. | Define a **clustering key** on the external Iceberg table: `ALTER EXTERNAL ICEBERG TABLE semantic_events CLUSTER BY (device_id);`. Snowflake will maintain its own clustering metadata without rewriting the underlying files. |
| **Result‑set caching** | Snowflake caches query results for a short window; Iceberg metadata changes invalidate the cache. | After a schema promotion, run a cheap `SELECT 1 FROM semantic_events LIMIT 1;` to warm the cache. |
| **Column pruning** | Iceberg stores all columns in the same Parquet files, but Snowflake reads only the columns referenced in the query. | Encourage analysts to select only needed columns (e.g., `SELECT device_id, temperature FROM …`). This reduces I/O and speeds up queries. |
| **File size** | Iceberg recommends ~128 MiB–1 GiB file sizes for optimal scan performance. | Verify your Firehose → Iceberg write settings (`target_file_size_bytes`). Snowflake’s scan cost scales with the number of files, not total data size. |
| **Metadata refresh frequency** | Frequent schema changes cause many refreshes; each refresh incurs a small catalog‑update cost. | Batch schema promotions (e.g., nightly) when possible, or use a **debounce** Lambda that waits a few minutes after the last drift event before calling `REFRESH`. |

---

## 4. End‑to‑end “new‑field” flow that satisfies both groups

```
Device → SQS → ingest Lambda → Firehose → Iceberg RAW
                     │
                     ├─► Curated (auto‑evolve)  <-- data scientists query via Athena/Presto/EMR
                     │
                     └─► Semantic (strict)  <-- Snowflake external Iceberg table
                         │
                         ├─► If column missing → write to drift_log (DynamoDB)
                         │
                         └─► Promotion workflow:
                               1️⃣ Review drift_log entry
                               2️⃣ Add column via Terraform/Glue DDL (typed)
                               3️⃣ Run `ALTER EXTERNAL ICEBERG TABLE semantic_events REFRESH;`
                               4️⃣ (Optional) Run `CLUSTER BY` if column is high‑cardinality
                               5️⃣ Analysts now see the new typed column instantly
```

*Data scientists* never wait – they read the **Curated** table, which already contains the new field as a `STRING`.  
*Analysts* see the new column only after step 3, which guarantees that the column has passed governance (type, validation, PII masking, etc.) before it appears in Snowflake.

---

## 5. Alternative designs (if you want to avoid a separate “Curated” table for Snowflake)

| Design | How new fields become visible to Snowflake | Pros | Cons |
|--------|--------------------------------------------|------|------|
| **Single Iceberg table with a `MAP<STRING,STRING>` column** (e.g., `attributes MAP<STRING,STRING>`) | Snowflake can query map entries via `attributes['new_key']`. No DDL needed for new keys. | Simplifies schema management; no promotion workflow. | Queries become less readable (`SELECT attributes['battery_level']`). Analysts lose typed columns; you must cast manually. |
| **Two‑stage Snowflake view** – a **raw view** that selects `*` from the Iceberg table (exposes all columns) and a **governed view** that selects only approved columns. | New columns appear instantly in the raw view; the governed view is updated only after promotion. | Mirrors the Curated ↔ Semantic split inside Snowflake itself. | Still requires a refresh step for the raw view; you lose the “auto‑evolve” convenience of the Curated Iceberg table for data‑scientist notebooks. |
| **Materialized view** that flattens the Iceberg table into a **wide, typed** Snowflake table on a schedule (e.g., every hour). | New columns are added to the materialized view only after the scheduled run, giving analysts a stable schema. | Guarantees high performance (Snowflake stores the data locally). | Extra storage cost; latency between ingestion and analyst visibility. |

If you prefer to keep the current **Curated → Semantic** split, the first option (the one we described above) is the most straightforward and gives the best balance of latency, governance, and performance.

---

## 6. Quick‑start checklist for Snowflake integration

1. **Create the external stage** (once):
   ```sql
   CREATE STAGE lean_ops_stage
     URL='s3://my-bucket/iceberg/'
     STORAGE_INTEGRATION = my_aws_integration;
   ```
2. **Create the external Iceberg table** (once):
   ```sql
   CREATE EXTERNAL ICEBERG TABLE lean_ops.semantic_events
     WITH LOCATION='s3://my-bucket/iceberg/semantic/'
     USING CATALOG 'glue';
   ```
3. **Add partition pruning** (optional but recommended):
   ```sql
   ALTER EXTERNAL ICEBERG TABLE lean_ops.semantic_events
     SET PARTITIONING = (period_reference, day(publish_time));
   ```
4. **Set up the “drift‑log → refresh” automation**
   * Lambda triggered by a DynamoDB stream on `drift_log` → calls Snowflake’s **REST API** (`/queries/v1`) with:
     ```sql
     ALTER EXTERNAL ICEBERG TABLE lean_ops.semantic_events REFRESH;
     ```
   * Debounce the trigger (e.g., wait 5 min after the last entry) to avoid excessive refreshes.
5. **Add a clustering key** for any high‑cardinality column you know will be filtered often (e.g., `device_id`).
   ```sql
   ALTER EXTERNAL ICEBERG TABLE lean_ops.semantic_events CLUSTER BY (device_id);
   ```
6. **Document the workflow** in `docs/LESSONS_LEARNED.md` (add a “Snowflake refresh” section) and update the README diagram to show the Snowflake external Iceberg arrow.

---

### TL;DR for the two consumer groups with Snowflake in the loop

| Consumer | Table they query | When they see a new column |
|----------|------------------|----------------------------|
| **Data Scientists** | `lean_ops.curated_events` (Athena/Presto) | **Immediately** – auto‑evolve adds a `STRING` column. |
| **Analysts (Snowflake)** | `lean_ops.semantic_events` (External Iceberg) | **After promotion** – a governance‑approved DDL adds the typed column, then a `REFRESH` makes it visible in Snowflake. |

By keeping the **auto‑evolve Curated layer** for rapid exploration and a **strict, versioned Semantic layer** that Snowflake reads as an external Iceberg table, you get low‑latency access for scientists and a governed, high‑performance surface for analysts — all while preserving a single source of truth in S3.
