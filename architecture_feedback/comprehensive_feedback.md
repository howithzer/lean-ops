# Comprehensive Architecture Feedback

## Executive Summary

This document consolidates all architecture recommendations for the **lean-ops** IoT data pipeline, including:
- Step Function orchestration design
- Partitioning & performance tuning
- Snowflake integration
- Repository hygiene

---

## 1. Step Function Orchestration Design

### Optimization Summary

| Original | Optimized | Benefit |
|----------|-----------|---------|
| 3 separate checkpoint calls | **1 parallel call** returns all checkpoints | Fewer cold starts, reduced latency |
| `GetCuratedSnapshot` after Curation | **Token passed** from job output | Eliminates race condition |
| Multiple schema lookups | **Schema info in initial call** | Single source of truth |

---

### Optimized Flow (Final Design)

```
                                    Start
                                      │
                                      ▼
                         ┌────────────────────────┐
                         │   GetAllCheckpoints    │  ◄── Single Lambda returns:
                         │   (Parallel Lookup)    │      • raw_snapshot
                         └────────────────────────┘      • curated_checkpoint
                                      │                  • semantic_checkpoint
                                      │                  • schema_exists (bool)
                                      ▼
                         ┌────────────────────────┐
                         │   CheckForNewRawData   │  ◄── $.raw_snapshot vs $.curated_checkpoint
                         └────────────────────────┘
                              │              │
                         No   │              │ Yes
                              ▼              ▼
                         NoNewData    ┌────────────────────────┐
                                      │    RunCurationJob      │
                                      └────────────────────────┘
                                                   │
                                                   │ Output: {
                                                   │   curated_snapshot_id,
                                                   │   records_written
                                                   │ }
                                                   ▼
                         ┌────────────────────────┐
                         │ UpdateCurationCheckpoint│  ◄── Uses $.curated_snapshot_id
                         └────────────────────────┘
                                      │
                                      ▼
                         ┌────────────────────────┐
                         │   CheckSchemaExists    │  ◄── Uses $.schema_exists
                         └────────────────────────┘
                              │              │
                         No   │              │ Yes
                              ▼              ▼
              ┌─────────────────────┐  ┌────────────────────────┐
              │NotifyMissingSchema  │  │ CheckForNewCuratedData │  ◄── $.curated_snapshot_id
              └─────────────────────┘  └────────────────────────┘      vs $.semantic_checkpoint
                        │                     │              │
                        ▼                No   │              │ Yes
              CurationOnlySuccess             ▼              ▼
                                      NoNewCuratedData ┌────────────────────────┐
                                                       │   RunSemanticJob       │
                                                       └────────────────────────┘
                                                                  │
                                                                  │ Input: $.curated_snapshot_id
                                                                  │ Output: { records_written }
                                                                  ▼
                                                       ┌────────────────────────┐
                                                       │UpdateSemanticCheckpoint│
                                                       └────────────────────────┘
                                                                  │
                                                                  ▼
                                                       ┌────────────────────────┐
                                                       │  CheckBacklogThreshold │  ◄── records > 10000?
                                                       └────────────────────────┘
                                                            │              │
                                                       No   │              │ Yes
                                                            ▼              ▼
                                                   FullPipelineSuccess   Loop → Start
```

---

### State Input/Output Mapping

```json
// GetAllCheckpoints output (stored in $)
{
  "raw_snapshot": "snap-001",
  "curated_checkpoint": "snap-000",
  "semantic_checkpoint": "snap-000",
  "schema_exists": true
}

// RunCurationJob output (merged into $)
{
  "curated_snapshot_id": "snap-002",
  "records_written": 5000
}

// RunSemanticJob input (from $)
{
  "source_snapshot": "$.curated_snapshot_id",
  "target_checkpoint": "$.semantic_checkpoint"
}
```

---

### Key States Explained

#### 1. GetAllCheckpoints (Single Lambda)
Replaces 4 separate lookups with one call that queries:
- Iceberg metadata for RAW/Curated/Semantic snapshots
- DynamoDB for schema_exists flag

#### 2. NotifyMissingSchema
When schema doesn't exist, sends:
- CloudWatch metric: `Topic [XYZ] missing Semantic Schema`
- DynamoDB entry to `drift_log` table

#### 3. CheckBacklogThreshold
If `records_written > 10000`, loops back to `Start` immediately instead of waiting for next EventBridge trigger

```
FullPipelineSuccess
    │
    └─► Choice: records_written > 10000?
            │
            ├─► Yes → Loop back to Start (immediate re-run)
            └─► No  → End (wait for next scheduled trigger)
```

**Benefit:** Pipeline "churns" through backlog without waiting for the 15-minute EventBridge trigger.

---

## 2. Partitioning Strategy by Layer

| Layer | Partitioning | Bloom Filters | Write Mode |
|-------|--------------|---------------|------------|
| **RAW** | `day(publish_time)` | ❌ | APPEND |
| **Curated** | `period_reference`, `day(publish_time)`, `bucket(16, idempotency_key)` | ✅ on `idempotency_key` | MERGE |
| **Semantic** | `period_reference`, `day(publish_time)`, `bucket(16, idempotency_key)` | ✅ on `idempotency_key` | MERGE |

### DDL Examples

**RAW:**
```sql
PARTITIONED BY (day(publish_time))
```

**Curated & Semantic:**
```sql
PARTITIONED BY (
    period_reference,
    day(publish_time),
    bucket(16, idempotency_key)
)
TBLPROPERTIES (
    'write.parquet.bloom-filter-enabled.column.names' = 'idempotency_key'
)
```

---

## 3. Schema Evolution Strategy

| Layer | Schema Behavior | New Column Handling |
|-------|-----------------|---------------------|
| **RAW** | Agnostic (`json_payload` column) | N/A – full JSON preserved |
| **Curated** | Auto-evolve (`schema.auto-evolve = true`) | New STRING columns added automatically |
| **Semantic** | Strict (`schema.auto-evolve = false`) | Logged to `drift_log`, requires DDL approval |

---

## 4. Snowflake Integration

| Object | Configuration |
|--------|---------------|
| External Iceberg Table | Points to Semantic layer metadata location |
| Refresh Trigger | Lambda on `drift_log` DynamoDB stream → calls `ALTER EXTERNAL ICEBERG TABLE ... REFRESH` |
| Clustering | `CLUSTER BY (device_id)` for high-cardinality filter columns |

---

## 5. Observability Enhancements

| Metric | Source | Alert Threshold |
|--------|--------|-----------------|
| DLQ Age | CloudWatch (SQS `ApproximateAgeOfOldestMessage`) | > 7 days |
| Schema Drift | DynamoDB `drift_log` table | Any new entry |
| Curation Lag | Step Function output (`records_written`) | > 100,000 records pending |
| Semantic Lag | Curated snapshot vs Semantic checkpoint | > 2 hours |

---

## 6. Repository Cleanup

| Path | Action |
|------|--------|
| `.terraform/` | Delete, add to `.gitignore` |
| `tfplan` | Delete, add to `.gitignore` |
| `modules/semantic/`, `modules/unified/` | Remove (unused) |
| `modules/compute/.build/` | Delete, add to `.gitignore` |
| `terraform.tfstate*` | Delete, never commit |

---

## 7. CI/CD Pipeline

1. **Lint** – `terraform fmt -check`, `tflint`
2. **Unit Tests** – `pytest` for Lambda code
3. **Integration** – Localstack + data injector
4. **Security** – `checkov`, `bandit`
5. **Deploy** – Manual approval on `main` merge

---

## 8. Development Checklist

- [ ] Refactor RAW ingest: APPEND only, no merge logic
- [ ] Update Curated DDL: `schema.auto-evolve = true`, 3-level partitioning
- [ ] Update Semantic DDL: Strict schema, 3-level partitioning, bloom filters
- [ ] Implement Curation Glue job: MERGE with `event_timestamp` check
- [ ] Implement Semantic Glue job: MERGE with governance validation
- [ ] Add Step Function optimizations (drift notification, token passing, backlog loop)
- [ ] Create Scientist View: `QUALIFY ROW_NUMBER()` dedup view on Curated
- [ ] Set up Snowflake external Iceberg table + refresh automation
- [ ] Add DLQ age alarm (7-day)
- [ ] Add schema drift notification

---

*Prepared: 2026-01-11*
