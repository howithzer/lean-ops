# Lean-Ops Data Pipeline Architecture

> **Version**: 1.0.0  
> **Last Updated**: 2026-01-19  
> **Purpose**: Developer onboarding and reference documentation

---

## Table of Contents

1. [Overview](#overview)
2. [Data Zones](#data-zones)
3. [Data Flow](#data-flow)
4. [Step Functions Orchestration](#step-functions-orchestration)
5. [Error Handling](#error-handling)
6. [Schema Management](#schema-management)
7. [Operational Runbook](#operational-runbook)
8. [Roadmap](#roadmap)

---

## Overview

This pipeline ingests event data from SQS, processes it through three data zones (RAW → Standardized → Curated), and uses Apache Iceberg tables for ACID transactions and time-travel capabilities.

### Technology Stack

| Component | Technology |
|-----------|------------|
| **Ingestion** | SQS → Lambda → Kinesis Firehose → Iceberg |
| **Processing** | AWS Glue Spark ETL |
| **Storage** | Apache Iceberg on S3 |
| **Orchestration** | AWS Step Functions |
| **Checkpointing** | DynamoDB |
| **Schema Registry** | S3 JSON files |

### Key Design Principles

1. **Schema-Gated Processing**: No processing until schema is deployed
2. **Two-Stage Deduplication**: FIFO (network duplicates) + LIFO (app corrections)
3. **100% Data Accountability**: Every record is tracked (processed or error-routed)
4. **Incremental Processing**: Checkpoint-based to avoid reprocessing

---

## Data Zones

### Multi-Topic Architecture

The pipeline supports **multiple topics**, each with its own tables at each layer:

```
┌───────────────┬─────────────────────┬─────────────────────┬─────────────────────┐
│    TOPIC      │       RAW           │   STANDARDIZED      │     CURATED         │
├───────────────┼─────────────────────┼─────────────────────┼─────────────────────┤
│   events      │ events_staging      │ events              │ events              │
│   orders      │ orders_staging      │ orders (TBD)        │ orders (TBD)        │
│   payments    │ payments_staging    │ payments (TBD)      │ payments (TBD)      │
└───────────────┴─────────────────────┴─────────────────────┴─────────────────────┘

Note: (TBD) = Table structure ready, schema not yet deployed
```

> **Important**: Each topic flows through its own set of tables. The Step Function is parameterized with `topic_name` and dynamically routes to the correct tables and schemas.

### Zone Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA ZONES                                    │
├─────────────┬─────────────────────────┬─────────────────────────────────┤
│    RAW      │     STANDARDIZED        │         CURATED                 │
│  (Landing)  │    (Conformed)          │      (Business-Ready)           │
├─────────────┼─────────────────────────┼─────────────────────────────────┤
│ • Direct    │ • Flattened JSON        │ • Typed columns                 │
│   Firehose  │ • ALL STRING            │ • CDE validated                 │
│   writes    │ • Network dedup         │ • MERGE on idempotency_key      │
│ • No schema │ • Dynamic schema        │ • Schema-driven                 │
│   required  │   evolution             │                                 │
└─────────────┴─────────────────────────┴─────────────────────────────────┘
```

### Zone Details

#### 1. RAW Zone (`iceberg_raw_db`)

**Purpose**: Landing zone for raw event data. Data lands here regardless of processing status.

**Tables**:
- `events_staging` - Raw events from Firehose
- `orders_staging` - Raw orders (future)
- `payments_staging` - Raw payments (future)

**Columns**:
```sql
message_id      STRING   -- Unique message ID from source
topic_name      STRING   -- Source topic (events, orders, etc.)
json_payload    STRING   -- Full JSON payload as string
ingestion_ts    BIGINT   -- Epoch timestamp of landing
```

**Important**: Data lands in RAW even if schema is not deployed. Processing only happens when schema exists.

---

#### 2. Standardized Zone (`iceberg_standardized_db`)

**Purpose**: Conformed, flattened data with network deduplication applied.

**Tables**:
- `events` - Standardized events (all columns STRING)
- `parse_errors` - Records that failed JSON parsing

**Processing Logic** (Target Design - Snapshot-Based):
1. Get current RAW snapshot ID
2. Read incremental data: `start-snapshot-id` → `end-snapshot-id` (Iceberg time-travel)
3. FIFO deduplication on `message_id` (removes network duplicates)
4. Flatten nested JSON into columns (e.g., `event.userId` → `event_userid`)
5. Map metadata columns (e.g., `_metadata_idempotencykeyresource` → `idempotency_key`)
6. Schema evolution: add new columns dynamically
7. MERGE into Standardized table
8. Save snapshot ID to DynamoDB checkpoint

> ⚠️ **Note**: Current implementation uses timestamp-based reads (`ingestion_ts > checkpoint`). Migration to snapshot-based is on the roadmap.

**Key Columns After Flattening**:
```
message_id, idempotency_key, publish_time, ingestion_ts, topic_name,
event_timestamp, event_userid, event_eventtype, event_verb, sessionid,
event_metadata_ipaddress, event_metadata_deviceid, event_metadata_useragent
```

---

#### 3. Curated Zone (`iceberg_curated_db`)

**Purpose**: Business-ready data with typed columns and CDE (Critical Data Element) validation.

**Tables**:
- `events` - Curated events (typed, validated)
- `errors` - CDE validation failures

**Processing Logic** (Timestamp-Based - Curated uses MERGE, not append-only):
1. Read from Standardized where `ingestion_ts > last_checkpoint`
2. Validate CDEs (required fields that cannot be NULL)
3. Route invalid records to `errors` table
4. Cast STRING columns to proper types (INT, TIMESTAMP, DECIMAL)
5. MERGE into Curated table on `idempotency_key`

> **Note**: Curated uses timestamp-based reads because MERGE invalidates snapshots. Snapshot-based reads are for append-only tables (RAW).

**Schema-Driven**: Uses `schemas/curated_schema.json` for:
- Column definitions and types
- CDE flags (required fields)
- Default values

---

## Data Flow

```
                                    ┌──────────────────┐
                                    │   SQS Queue      │
                                    │ (events-queue)   │
                                    └────────┬─────────┘
                                             │
                                             ▼
                                    ┌──────────────────┐
                                    │  Lambda Trigger  │
                                    │ (SQS → Firehose) │
                                    └────────┬─────────┘
                                             │
                                             ▼
                                    ┌──────────────────┐
                                    │  Kinesis Firehose│
                                    │ (Direct to       │
                                    │  Iceberg)        │
                                    └────────┬─────────┘
                                             │
           ┌─────────────────────────────────┼─────────────────────────────────┐
           │                                 │                                 │
           │                                 ▼                                 │
           │                        ┌──────────────────┐                       │
           │                        │    RAW TABLE     │                       │
           │                        │ events_staging   │                       │
           │                        └────────┬─────────┘                       │
           │                                 │                                 │
           │              ┌──────────────────┼──────────────────┐              │
           │              │                  │                  │              │
           │              ▼                  ▼                  ▼              │
           │     ┌────────────────┐  ┌──────────────┐  ┌────────────────┐      │
           │     │ Schema Check   │  │  If Schema   │  │  If No Schema  │      │
           │     │ (events.json)  │──│    EXISTS    │  │  SKIP TO END   │      │
           │     └────────────────┘  └──────┬───────┘  └────────────────┘      │
           │                                │                                  │
           │                                ▼                                  │
           │                       ┌──────────────────┐                        │
           │                       │ STANDARDIZED JOB │                        │
           │                       │ • FIFO Dedup     │                        │
           │                       │ • Flatten JSON   │                        │
           │                       │ • Schema Evolve  │                        │
           │                       └────────┬─────────┘                        │
           │                                │                                  │
           │                   ┌────────────┼────────────┐                     │
           │                   │            │            │                     │
           │                   ▼            ▼            ▼                     │
           │          ┌─────────────┐ ┌───────────┐ ┌─────────────┐            │
           │          │ STANDARDIZED│ │  PARSE    │ │   CURATED   │            │
           │          │   TABLE     │ │  ERRORS   │ │    JOB      │            │
           │          │  (events)   │ │  TABLE    │ │ • CDE Valid │            │
           │          └─────────────┘ └───────────┘ │ • Type Cast │            │
           │                                        │ • MERGE     │            │
           │                                        └──────┬──────┘            │
           │                                               │                   │
           │                                  ┌────────────┼────────────┐      │
           │                                  ▼            ▼            ▼      │
           │                          ┌─────────────┐ ┌───────────┐            │
           │                          │  CURATED    │ │   CDE     │            │
           │                          │   TABLE     │ │  ERRORS   │            │
           │                          │  (events)   │ │  TABLE    │            │
           │                          └─────────────┘ └───────────┘            │
           │                                                                   │
           └───────────────────────────────────────────────────────────────────┘
```

---

## Step Functions Orchestration

The Step Function runs on a 15-minute schedule (EventBridge) and orchestrates the full pipeline.

### State Machine Flow

```
START
  │
  ▼
┌─────────────────────────┐
│ 1. CheckSchemaExists    │  ← Check for schemas/{topic}.json in S3
│    (Lambda)             │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ 2. SchemaGate (Choice)  │
└───────────┬─────────────┘
            │
     ┌──────┴──────┐
     │             │
     ▼             ▼
  [No Schema]   [Schema Exists]
     │             │
     ▼             ▼
  ┌──────────┐  ┌─────────────────────────┐
  │SkipNoSchema│ │3. EnsureStandardizedTable│
  │ (Success) │  │    (Lambda - DDL)       │
  └──────────┘  └───────────┬─────────────┘
                            │
                            ▼
                ┌─────────────────────────┐
                │ 4. RunStandardized      │  ← Glue Job (RAW → Standardized)
                │    (Glue Sync)          │
                └───────────┬─────────────┘
                            │
                            ▼
                ┌─────────────────────────┐
                │ 5. CheckCuratedReady    │  ← Check for curated_schema.json
                │    (Lambda)             │
                └───────────┬─────────────┘
                            │
                     ┌──────┴──────┐
                     │             │
                     ▼             ▼
         [No Curated Schema] [Schema Exists]
                     │             │
                     ▼             ▼
        ┌────────────────────┐  ┌─────────────────────────┐
        │SuccessStandardized │  │ 6. RunCurated           │
        │      Only          │  │    (Glue Sync)          │
        └────────────────────┘  └───────────┬─────────────┘
                                            │
                                            ▼
                                ┌─────────────────────────┐
                                │ 7. SuccessFull          │
                                └─────────────────────────┘
```

### State Machine States

| State | Type | Purpose |
|-------|------|---------|
| `CheckSchemaExists` | Lambda | Verify `schemas/{topic}.json` exists in S3 |
| `SchemaGate` | Choice | Route based on schema existence |
| `SkipNoSchema` | Pass | End gracefully if no schema (data buffers in RAW) |
| `EnsureStandardizedTable` | Lambda | Create/verify Standardized table DDL |
| `RunStandardized` | Glue Sync | Execute RAW → Standardized processing |
| `CheckCuratedReady` | Lambda | Verify `curated_schema.json` exists |
| `CuratedSchemaChoice` | Choice | Route to Curated or end |
| `RunCurated` | Glue Sync | Execute Standardized → Curated processing |
| `HandleError` | SNS | Alert on failures |

---

## Error Handling

### Error Tables

| Table | Zone | Purpose |
|-------|------|---------|
| `iceberg_standardized_db.parse_errors` | Standardized | Invalid JSON records |
| `iceberg_curated_db.errors` | Curated | CDE validation failures |
| `iceberg_raw_db.dlq_errors` | RAW | DLQ-drained messages |

### Error Routing Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        ERROR ROUTING                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  RAW → Standardized                                             │
│    ├── Valid JSON ───────────────────────► Standardized Table   │
│    └── Invalid JSON ─────────────────────► parse_errors Table   │
│                                                                 │
│  Standardized → Curated                                         │
│    ├── CDE Valid ────────────────────────► Curated Table        │
│    └── CDE Violation ────────────────────► errors Table         │
│                                                                 │
│  SQS Lambda                                                     │
│    ├── Success ──────────────────────────► Firehose → RAW       │
│    └── Failure (5 retries) ──────────────► DLQ                  │
│          └── DLQ Drainer ────────────────► dlq_errors Table     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Accountability Formula

**Full Pipeline Accountability** (end-to-end):
```
┌─────────────────────────────────────────────────────────────────────────┐
│                     FULL ACCOUNTABILITY                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  SQS Messages Received                                                  │
│    ├── Lambda Success ────────────► Firehose                            │
│    │     ├── Firehose Success ───► RAW Table                            │
│    │     └── Firehose Error ─────► Firehose Error Bucket (*)            │
│    └── Lambda Failure (5x) ──────► DLQ ─► dlq_errors Table              │
│                                                                         │
│  RAW Records                                                            │
│    ├── Valid JSON ───────────────► Standardized Table                   │
│    └── Invalid JSON ─────────────► parse_errors Table                   │
│                                                                         │
│  Standardized Records                                                   │
│    ├── CDE Valid ────────────────► Curated Table                        │
│    └── CDE Violation ────────────► cde_errors Table                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

(*) Firehose errors land in S3 error prefix, not currently tracked in table
```

**Pre-RAW Accountability** (ingestion layer):
```
Pre_RAW_Accountability % = (RAW + DLQ_Errors + Firehose_Errors) / SQS_Messages × 100
```

**Post-RAW Accountability** (processing layer):
```
Post_RAW_Accountability % = (Standardized + Parse_Errors) / RAW × 100
```

**Curated Accountability**:
```
Curated_Accountability % = (Curated + CDE_Errors) / Standardized × 100
```

> ⚠️ **Current Implementation**: Only tracks Post-RAW accountability. Pre-RAW tracking (SQS metrics, Firehose errors) is on the roadmap.

**Target**: 100% accountability at each layer

---

## Schema Management

### Schema Files

| File | Location | Purpose |
|------|----------|---------|
| `schemas/events.json` | S3 + Git | Standardized layer schema (table structure) |
| `schemas/curated_schema.json` | S3 + Git | Curated layer schema (types, CDEs) |

### Schema Deployment

**Important**: Schema = "Topic is onboarded and ready for processing"

```bash
# Deploy schemas (enables processing)
./tests/e2e/production_emulation.sh schema

# This uploads:
# - schemas/events.json → s3://bucket/schemas/events.json
# - schemas/curated_schema.json → s3://bucket/schemas/curated_schema.json
```

### Schema Gate Behavior

| Scenario | Schema in S3? | Behavior |
|----------|---------------|----------|
| Day 1 (New Topic) | No | Data lands in RAW, processing skipped |
| Day 2 (Onboarded) | Yes | Full processing RAW → Standardized → Curated |

---

## Operational Runbook

### Deploy Infrastructure

```bash
cd lean-ops
./scripts/run_tests.sh deploy
```

### Deploy Schema (Enable Processing)

```bash
./tests/e2e/production_emulation.sh schema
```

### Run Full E2E Test

```bash
./tests/e2e/production_emulation.sh full
```

### Monitor Step Function

```bash
# Check execution
AWS_PROFILE=terraform-firehose aws stepfunctions describe-execution \
  --execution-arn <execution-arn> --region us-east-1

# View CloudWatch logs
AWS_PROFILE=terraform-firehose aws logs tail /aws-glue/jobs/output --follow
```

### Check Data Accountability

```bash
./tests/e2e/production_emulation.sh verify
```

### Destroy Infrastructure

```bash
./scripts/run_tests.sh destroy
```

---

## Roadmap

### ✅ Completed

- [x] RAW → Standardized → Curated pipeline
- [x] Schema-gated processing
- [x] FIFO/LIFO deduplication
- [x] CDE validation
- [x] Schema evolution (new columns)
- [x] Error routing (parse_errors, cde_errors)
- [x] E2E test framework

### 🔧 In Progress

- [ ] Unified error dashboard view
- [ ] Data quality metrics tracking

### 📋 Backlog (Not Implemented)

| Feature | Priority | Notes |
|---------|----------|-------|
| **Snapshot-Based Incremental Reads** | High | Migrate RAW→Standardized to use Iceberg snapshot IDs instead of `ingestion_ts`. Store snapshot_id in DynamoDB. |
| **Negative Test Cases** | High | `error_injection` config not implemented in data_injector |
| DLQ retry mechanism | Medium | Currently drains to table, no retry |
| Job failure alerting | Medium | SNS wired, needs tuning |
| Malformed timestamp testing | Low | Need test data |
| Performance benchmarks | Low | Need baseline metrics |

---

## Appendix

### Key File Locations

| File | Purpose |
|------|---------|
| `scripts/glue/standardized_processor.py` | RAW → Standardized Glue job |
| `scripts/glue/curated_processor.py` | Standardized → Curated Glue job |
| `modules/orchestration/main.tf` | Step Function definition |
| `modules/catalog/main.tf` | Iceberg table DDL |
| `schemas/events.json` | Standardized schema |
| `schemas/curated_schema.json` | Curated schema (CDEs, types) |
| `tests/e2e/production_emulation.sh` | E2E test runner |

### DynamoDB Checkpoint Table

**Current Schema** (Timestamp-based):
```
Table: lean-ops-dev-checkpoints

| pipeline_id | checkpoint_type | last_ingestion_ts | updated_at |
|-------------|-----------------|-------------------|------------|
| standardization_events | standardized | 1768875000 | 2026-01-19 |
| curated_events | curated | 1768875000 | 2026-01-19 |
```

**Target Schema** (Snapshot-based for RAW→Standardized):
```
Table: lean-ops-dev-checkpoints

| pipeline_id | checkpoint_type | last_snapshot_id | last_ingestion_ts | updated_at |
|-------------|-----------------|------------------|-------------------|------------|
| standardization_events | standardized | 8234567890123 | 1768875000 | 2026-01-19 |
| curated_events | curated | NULL | 1768875000 | 2026-01-19 |
```

> **Note**: `last_snapshot_id` used for append-only tables (RAW). `last_ingestion_ts` used for MERGE tables (Curated).

---

*For questions, contact the platform team.*
