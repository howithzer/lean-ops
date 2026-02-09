# Lean-Ops: Enterprise Event Hub (EEH) Architecture

> **Version**: 2.0 | **Date**: 2026-02-06 | **Status**: Phase 2 Complete, Phase 3 In Progress

---

## 1. Problem Statement

### Business Need

Move high-velocity IoT event data from **GCP Pub/Sub** (700+ topics) to an **AWS-managed Iceberg data lake** in near real-time, with:

- **Scalability**: Handle variable throughput across hundreds of topics
- **Governance**: Type-safe curated layer with CDE enforcement
- **Resilience**: No data loss, graceful error handling
- **Auditability**: Full lineage from source to curated
- **Flexibility**: Dynamic topic discovery and schema evolution

### Key Challenges

| Challenge | Solution |
|-----------|----------|
| Cross-cloud data movement | EKS bridge (GCP Pub/Sub → SQS) |
| Schema variability across topics | Dynamic Standardized layer (all STRING) |
| Network duplicates | FIFO deduplication on `message_id` |
| Business corrections | LIFO deduplication on `idempotency_key` |
| Late-arriving data | Snapshot-based incremental processing |
| Schema drift | Auto-add columns + drift logging |

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              GCP → AWS DATA FLOW                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌──────────┐    ┌─────────────┐   │
│  │  GCP    │    │  EKS    │    │   SQS   │    │  Lambda  │    │  Firehose   │   │
│  │ Pub/Sub │───▶│ Bridge  │───▶│  Queue  │───▶│ Processor│───▶│  (shared)   │   │
│  └─────────┘    └─────────┘    └─────────┘    └──────────┘    └──────┬──────┘   │
│                                     │                                 │         │
│                                     ▼                                 ▼         │
│                              ┌─────────────┐                  ┌─────────────┐   │
│                              │     DLQ     │                  │  RAW Layer  │   │
│                              │  (14 days)  │                  │ (Iceberg)   │   │
│                              └─────────────┘                  └──────┬──────┘   │
│                                                                      │          │
│                         ┌────────────────────────────────────────────┤          │
│                         │              Step Functions                │          │
│                         │              (every 15 min)                │          │
│                         │                                            │          │
│                         ▼                                            ▼          │
│                  ┌─────────────┐                              ┌─────────────┐   │
│                  │ Standardized│──────────────────────────────│   Curated   │   │
│                  │ (All STRING)│                              │   (Typed)   │   │
│                  └─────────────┘                              └─────────────┘   │
│                         │                                            │          │
│                         ▼                                            ▼          │
│                  ┌─────────────┐                              ┌─────────────┐   │
│                  │ parse_errors│                              │   errors    │   │
│                  └─────────────┘                              │  (CDE fail) │   │
│                                                               └─────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Flow

### 3.1 Ingestion (Pre-RAW)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ① GCP Pub/Sub                                                                 │
│     • Messages contain: message_id, publish_time, idempotency_key, payload      │
│                                                                                 │
│  ② EKS Bridge (Python containers)                                               │
│     • Streaming API pull from GCP                                               │
│     • Extract envelope: message_id, publish_time, idempotency_key               │
│     • Send to SQS                                                               │
│     • ACK sent to GCP after SQS confirmation                                    │
│                                                                                 │
│  ③ SQS Queue (per topic)                                                        │
│     • Buffers messages before Firehose consumption                              │
│     • Failed messages → DLQ (14-day retention)                                  │
│                                                                                 │
│  ④ Lambda Processor                                                             │
│     • Validates payload structure                                               │
│     • Extracts business keys                                                    │
│     • Invalid records → DLQ with circuit breaker                                │
│                                                                                 │
│  ⑤ Firehose                                                                     │
│     • Buffers: 64 MB or 300 seconds (whichever first)                           │
│     • Transformation Lambda adds topic routing                                  │
│     • Writes Parquet to RAW Iceberg table                                       │
│                                                                                 │
│  ⑥ RAW Iceberg Table                                                           │
│     • Append-only, immutable audit trail                                        │
│     • Partitioned by: day(publish_time)                                         │
│     • Columns: message_id, publish_time, topic_name, json_payload               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Standardization

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Triggered: Every 15 minutes via Step Functions                                 │
│                                                                                 │
│  ① Stage Gate Check                                                            │
│     • Prior batch successful?                                                   │
│     • Schema valid? (DynamoDB flag: processing_enabled)                         │
│                                                                                 │
│  ② Get Checkpoint                                                               │
│     • Read high watermark from DynamoDB                                         │
│     • Compare with RAW table snapshots                                          │
│     • If no new data → exit cleanly                                             │
│                                                                                 │
│  ③ Glue Job Processing                                                          │
│     • Read incremental data from RAW                                            │
│     • Validate JSON structure                                                   │
│     • Flatten nested payload → individual columns                               │
│     • FIFO dedup on message_id (network retries)                                │
│     • LIFO dedup on idempotency_key (business corrections)                      │
│                                                                                 │
│  ④ Schema Evolution                                                             │
│     • Detect new columns in payload                                             │
│     • Auto-add as STRING type                                                   │
│     • Log drift to ops_db.drift_log                                             │
│                                                                                 │
│  ⑤ Write Results                                                                │
│     • Valid records → Standardized table                                        │
│     • Parse errors → parse_errors table                                         │
│     • Update checkpoint in DynamoDB                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Curation

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Triggered: After Standardization completes                                     │
│                                                                                 │
│  ① Stage Gate Check                                                             │
│     • Standardization successful?                                               │
│     • Schema file exists in S3?                                                 │
│                                                                                 │
│  ② Get Checkpoint & Schema                                                      │
│     • Read Curated checkpoint from DynamoDB                                     │
│     • Load schema from S3: schemas/{topic}/active/schema.json                   │
│                                                                                 │
│  ③ Glue Job Processing                                                          │
│     • Read incremental data from Standardized                                   │
│     • Apply schema-defined transformations:                                     │
│       - CDE enforcement (required fields)                                       │
│       - NULL constraint checks                                                  │
│       - Type-safe casting (STRING → INT, DECIMAL, TIMESTAMP)                    │
│                                                                                 │
│  ④ Write Results                                                                │
│     • Valid records → Curated table (MERGE upsert)                              │
│     • CDE violations → errors table                                             │
│     • Type conversion failures → errors table                                   │
│     • Update checkpoint in DynamoDB                                             │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Storage Architecture

### 4.1 State Store (DynamoDB)

**Purpose**: High-frequency, transient operational state

| Table | Purpose | TTL |
|-------|---------|-----|
| `checkpoints` | High watermarks (topic → snapshot_id) | None |
| `locks` | Distributed locking for concurrent jobs | 5 min |
| `processing_enabled` | Schema validation gate | None |
| `idempotency_keys` | Lambda deduplication | 24 hours |

### 4.2 Operational Data Store (Iceberg)

**Purpose**: Long-term operational metadata, queryable via Athena

| Table | Purpose | Retention |
|-------|---------|-----------|
| `ops_db.topic_registry` | Dynamic topic discovery | Years |
| `ops_db.job_run_history` | Pipeline execution metrics | Years |
| `ops_db.schema_versions` | Schema change history | Years |
| `ops_db.drift_log` | Column additions/changes | Years |
| `ops_db.reconciliation_log` | Count validation | Years |

### 4.3 Data Store (Iceberg)

| Database | Tables | Purpose |
|----------|--------|---------|
| `iceberg_raw_db` | `{topic}_staging` | Immutable audit trail |
| `iceberg_standardized_db` | `{topic}`, `parse_errors` | Flattened, deduped |
| `iceberg_curated_db` | `{topic}`, `errors` | Typed, governed |

---

## 5. Schema Management Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ASYNC SCHEMA MANAGEMENT (Decoupled from Data Flow)                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │  S3: schemas/{topic}/                                                    │   │
│  │  ├── pending/schema.json     ◀── Dev uploads                            │   │
│  │  ├── active/schema.json      ◀── Validated, production                  │   │
│  │  ├── failed/error.json       ◀── Validation errors                      │   │
│  │  └── archive/                ◀── Previous versions                      │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                           EventBridge (S3 trigger)                              │
│                                      │                                          │
│                                      ▼                                          │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │  Schema Validator Lambda                                                 │   │
│  │  1. Validate JSON syntax                                                 │   │
│  │  2. Check required fields (table_name, envelope_columns, payload_columns)│   │
│  │  3. Create/update Iceberg tables via Athena                              │   │
│  │  4. Move to active/ or failed/                                           │   │
│  │  5. Set DynamoDB: processing_enabled = true/false                        │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Data flows check processing_enabled flag every 15 minutes                      │
│  Schema drift is rare; async process is sufficient                              │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Quality Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  RECONCILIATION (Placeholder for Future Implementation)                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Purpose: Validate data completeness across layers                              │
│                                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                      │
│  │     RAW      │    │ Standardized │    │   Curated    │                      │
│  │   100,000    │ ─▶ │    99,500    │ ─▶ │    99,000    │                      │
│  └──────────────┘    └──────────────┘    └──────────────┘                      │
│         │                   │                   │                               │
│         │                   ▼                   ▼                               │
│         │            ┌──────────────┐    ┌──────────────┐                      │
│         │            │ parse_errors │    │    errors    │                      │
│         │            │     500      │    │     500      │                      │
│         │            └──────────────┘    └──────────────┘                      │
│         │                   │                   │                               │
│         ▼                   ▼                   ▼                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Reconciliation Query (scheduled)                                       │   │
│  │  SELECT                                                                  │   │
│  │    (raw_count) = (std_count + parse_errors) AS layer1_balanced,         │   │
│  │    (std_count) = (curated_count + cde_errors) AS layer2_balanced        │   │
│  │  → Log to ops_db.reconciliation_log                                      │   │
│  │  → Alert if variance > threshold                                         │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Governance Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  OPERATIONAL MONITORING & ALERTING                                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │  Job Performance Monitoring              │                                   │
│  │  • Duration trends                       │                                   │
│  │  • Record counts                         │                                   │
│  │  • Error rates                           │                                   │
│  │  → Stored in: ops_db.job_run_history     │                                   │
│  └─────────────────────────────────────────┘                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │  Drift Detection                         │                                   │
│  │  • New columns detected                  │                                   │
│  │  • Type changes attempted                │                                   │
│  │  → Stored in: ops_db.drift_log           │                                   │
│  └─────────────────────────────────────────┘                                   │
│                                                                                 │
│  ┌─────────────────────────────────────────┐                                   │
│  │  Alerting (CloudWatch + SNS)             │                                   │
│  │  • Job failures                          │                                   │
│  │  • Error rate > 5%                       │                                   │
│  │  • Reconciliation variance > 1%          │                                   │
│  │  • Schema validation failures            │                                   │
│  └─────────────────────────────────────────┘                                   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Maintenance Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  ICEBERG TABLE MAINTENANCE                                                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Daily Maintenance (EventBridge → Lambda/Glue)                           │   │
│  │                                                                          │   │
│  │  1. Expire Snapshots                                                     │   │
│  │     CALL system.expire_snapshots(table, retain_last => 20)               │   │
│  │                                                                          │   │
│  │  2. Rewrite Manifests                                                    │   │
│  │     CALL system.rewrite_manifests(table)                                 │   │
│  │                                                                          │   │
│  │  3. Compact Small Files (hot partitions)                                 │   │
│  │     CALL system.rewrite_data_files(table, options => {                   │   │
│  │       'target-file-size-bytes' => '268435456'  -- 256 MB                 │   │
│  │     })                                                                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Weekly Maintenance                                                      │   │
│  │                                                                          │   │
│  │  1. Remove Orphan Files                                                  │   │
│  │     CALL system.remove_orphan_files(table, older_than => '72 hours')     │   │
│  │                                                                          │   │
│  │  2. Rewrite Position Deletes (after MERGE operations)                    │   │
│  │     CALL system.rewrite_position_delete_files(table)                     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Table Properties for Optimization:                                             │
│  • write.target-file-size-bytes = 268435456 (256 MB)                           │
│  • write.distribution-mode = range                                              │
│  • commit.manifest.target-size-bytes = 8388608 (8 MB)                          │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 9. Architecture Decision Records (ADRs)

### ADR-001: State Store Technology

| Decision | DynamoDB for transient state |
|----------|------------------------------|
| **Context** | Need high-frequency reads/writes for checkpoints and locks |
| **Options** | DynamoDB, RDS PostgreSQL, Redis |
| **Decision** | DynamoDB |
| **Rationale** | - Serverless (no connection pooling issues with Lambda)<br>- Sub-ms latency<br>- Conditional writes for distributed locking<br>- TTL for automatic cleanup |

### ADR-002: ODS Technology

| Decision | Iceberg tables for operational metadata |
|----------|----------------------------------------|
| **Context** | Need long-term storage for job history, drift logs, reconciliation |
| **Options** | RDS PostgreSQL, Iceberg, DynamoDB |
| **Decision** | Iceberg |
| **Rationale** | - Single technology across data + ops<br>- Zero idle cost (pay per query)<br>- Same compaction strategy<br>- Query via Athena or Snowflake<br>- Years of retention at low cost |

### ADR-003: Schema Evolution Strategy

| Decision | All-STRING Standardized layer + typed Curated layer |
|----------|-----------------------------------------------------|
| **Context** | Need to handle schema drift gracefully while providing type safety |
| **Decision** | Standardized = all STRING (auto-evolving), Curated = typed (governed) |
| **Rationale** | - New fields available immediately in Standardized<br>- Type safety enforced at Curated layer<br>- Drift logged but doesn't break pipeline |

### ADR-004: Deduplication Strategy

| Decision | Two-stage deduplication |
|----------|-------------------------|
| **Context** | Network retries and business corrections both create duplicates |
| **Decision** | FIFO on message_id (Standardized) + LIFO on idempotency_key (Curated) |
| **Rationale** | - Network retries: keep first (they're identical)<br>- Business corrections: keep latest (it's the fix) |

---

## 10. Implementation Roadmap

### Completed ✅

| Phase | Features | Status |
|-------|----------|--------|
| **Wave 1-3** | SQS Processor, Firehose Transform, DLQ, Circuit Breaker | ✅ Deployed |
| **Wave 4** | Step Functions, Standardized/Curated Glue jobs | ✅ Deployed |
| **Phase 1** | DynamoDB schema-registry, processing_enabled flag | ✅ Complete |
| **Phase 2** | Schema folder structure, validation scripts, E2E tests | ✅ Complete |

### In Progress 🔄

| Phase | Features | Status |
|-------|----------|--------|
| **Phase 3** | Serverless table DDL from schema, drift logging | 🔄 Planned |

### Planned 📋

| Phase | Features | Priority |
|-------|----------|----------|
| **Quality Flow** | Reconciliation queries, variance alerting | High |
| **Governance Flow** | Job metrics to Iceberg, drift monitoring | Medium |
| **Maintenance Flow** | Compaction Lambda, scheduled maintenance | Medium |
| **Dynamic Discovery** | GCP topic scanner → Iceberg registry → Terraform trigger | Future |
| **Self-Healing** | Error replay, DLQ drain, auto-retry | Future |

---

## 11. Current Status

### Code Inventory

| Component | Path | Status |
|-----------|------|--------|
| EKS Bridge | External repo | ✅ Built/Tested |
| SQS Processor Lambda | `modules/compute/lambda/sqs_processor/` | ✅ Deployed |
| Firehose Transform | `modules/compute/lambda/firehose_transform/` | ✅ Deployed |
| GCP Topic Discovery | `modules/compute/lambda/topic_discovery/` | ✅ Built |
| Standardized Glue | `scripts/glue/standardized_processor.py` | ✅ Deployed |
| Curated Glue | `scripts/glue/curated_processor.py` | ✅ Deployed |
| Schema Evolution | `scripts/glue/utils/schema_evolution.py` | ✅ Deployed |
| Schema Validator | `modules/compute/lambda/schema_validator/` | 🔄 Placeholder |
| Compaction Job | Not started | 📋 Planned |

### Infrastructure

| Resource | Status |
|----------|--------|
| DynamoDB tables | ✅ Deployed |
| Glue databases | ✅ Deployed |
| Step Functions | ✅ Deployed |
| EventBridge (15-min) | ✅ Deployed |
| S3 schema folders | ✅ Created |

---

## 12. File Reference

| Category | Files |
|----------|-------|
| **Terraform** | `main.tf`, `modules/compute/`, `modules/orchestration/` |
| **Lambdas** | `modules/compute/lambda/*/handler.py` |
| **Glue Jobs** | `scripts/glue/*.py` |
| **Schemas** | `schemas/{topic}/active/schema.json` |
| **Tests** | `tests/e2e/production_emulation.sh`, `tests/unit/` |
| **Docs** | `docs/ARCHITECTURE.md`, `docs/layer_definitions.md` |
