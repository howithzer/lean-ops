# Lean-Ops: Unified Architecture

> **Version**: 2026-02-06 | **Phases**: 1-2 Complete, Phase 3 In Progress

---

## Vision

A **flexible, scalable, serverless** data platform that moves IoT event data through governed layers to Apache Iceberg tables with:
- **Automatic schema evolution** at Standardized layer
- **Type-safe governance** at Curated layer  
- **Drift detection and logging**
- **Self-healing and operational controls** (future)

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  EKS Pods (700+ topics)                                                         │
│       │                                                                         │
│       ▼                                                                         │
│  ┌─────────┐    ┌─────────┐    ┌──────────┐    ┌────────────────┐              │
│  │   SQS   │───▶│ Lambda  │───▶│ Firehose │───▶│   RAW Layer    │              │
│  │  Queue  │    │ Process │    │ (shared) │    │ (Iceberg V2)   │              │
│  └─────────┘    └─────────┘    └──────────┘    └───────┬────────┘              │
│       │                                                │                        │
│       ▼                                                ▼                        │
│  ┌─────────┐                            ┌────────────────────────┐              │
│  │   DLQ   │                            │  Step Functions + Glue │              │
│  └────┬────┘                            │  (15-min Schedule)     │              │
│       │                                 └────────────┬───────────┘              │
│       ▼                                              │                          │
│  ┌─────────────┐                    ┌────────────────┴────────────────┐         │
│  │ DLQ Archive │                    │                                 │         │
│  │ (S3 + DDB)  │                    ▼                                 ▼         │
│  └─────────────┘            ┌─────────────────┐              ┌─────────────────┐│
│                             │  STANDARDIZED   │              │    CURATED      ││
│                             │  (All STRING)   │              │  (Typed/Governed││
│                             │  Auto-evolving  │─────────────▶│   CDE Validated)││
│                             └─────────────────┘              └─────────────────┘│
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Schema Management Flow (Phase 2+)

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         SCHEMA MANAGEMENT (Serverless)                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  S3: schemas/{topic}/                                                           │
│  ├── pending/schema.json     ◀── Dev uploads here                              │
│  ├── active/schema.json      ◀── Validated, production-ready                    │
│  ├── failed/schema.json      ◀── Validation failed                              │
│  │       └── error.json      ◀── Feedback for dev                               │
│  └── archive/                ◀── Previous versions                              │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Upload to pending/  ─▶  EventBridge  ─▶  schema_validator Lambda       │   │
│  │                                               │                          │   │
│  │                                 ┌─────────────┴─────────────┐            │   │
│  │                                 ▼                           ▼            │   │
│  │                           [VALID]                      [INVALID]         │   │
│  │                               │                             │            │   │
│  │                               ▼                             ▼            │   │
│  │                    Move to active/               Move to failed/         │   │
│  │                    Create tables (Phase 3)       Write error.json        │   │
│  │                    Set DynamoDB flag=true        Set DynamoDB status=ERR │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  DynamoDB: schema-registry                                                      │
│  ┌────────────┬─────────────────────┬────────────┬──────────────┐              │
│  │ topic_name │ processing_enabled  │ status     │ updated_at   │              │
│  ├────────────┼─────────────────────┼────────────┼──────────────┤              │
│  │ events     │ true                │ READY      │ 2026-02-06   │              │
│  │ orders     │ false               │ ERROR      │ 2026-02-05   │              │
│  └────────────┴─────────────────────┴────────────┴──────────────┘              │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Layers

| Layer | Purpose | Schema | Deduplication | Consumers |
|-------|---------|--------|---------------|-----------|
| **RAW** | Audit trail | JSON blob | None | Engineers |
| **Standardized** | Exploration | All STRING, auto-evolving | FIFO (message_id) + LIFO | Data Scientists |
| **Curated** | Production | Typed, governed | LIFO (idempotency_key) | BI/Analysts |

### Layer Flow
```
RAW (immutable) → Standardized (flat, flexible) → Curated (typed, governed)
                        │
                        ├── drift_log (schema changes)
                        └── errors (CDE violations)
```

---

## Implementation Phases

| Phase | Status | Scope |
|-------|--------|-------|
| 1 | ✅ Complete | DynamoDB flag, Step Function integration |
| 2 | ✅ Complete | S3 folders, validation scripts, E2E tests |
| 3 | 🔄 In Progress | **Serverless table DDL**, drift logging, auto-backfill |
| 4 | 📋 Planned | REBUILD, rollback, maintenance mode, compaction |

---

## Test Coverage

### Current Tests

| Test Type | File | Coverage |
|-----------|------|----------|
| Unit | `tests/test_common.py` | Common library (topic_utils, error_classification) |
| Unit | `tests/unit/test_schema_registry_phase2.sh` | Schema registration workflow |
| E2E | `tests/e2e/production_emulation.sh` | Full pipeline (Day 1→Schema→Day 2) |
| Validation | `tests/validation/` | Athena query verification |

### Test Scenarios (Production Emulation)

| Scenario | Expected Behavior |
|----------|-------------------|
| No schema | Step Function skips Curated |
| Empty flow | "0 records" → exits cleanly |
| Invalid schema | Error logged, schema → failed/ |
| Parse errors | → `parse_errors` table |
| CDE violations | → `errors` table |
| Corrections | LIFO merge (latest wins) |
| Schema drift | Auto-add columns (Standardized) |

---

## Phase 3: Serverless Table Management

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        PHASE 3: TABLE DDL INTEGRATION                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  schema_validator Lambda (enhanced):                                            │
│                                                                                 │
│  1. Validate schema JSON                                                        │
│  2. Compare with existing (detect drift)                                        │
│  3. Create Standardized table via Athena                                        │
│  4. Create Curated table via Athena                                             │
│  5. Log drift to drift_log table                                                │
│  6. Move schema to active/                                                      │
│  7. Set DynamoDB processing_enabled=true                                        │
│                                                                                 │
│  Historical catch-up job:                                                       │
│  - Backfill drift_log for existing columns                                      │
│  - One-time migration for pre-Phase-3 topics                                    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Future: Self-Healing & Operational Controls (Phase 4+)

| Feature | Description |
|---------|-------------|
| **REBUILD command** | Reprocess topic from RAW with new schema |
| **Iceberg rollback** | Revert to previous snapshot on failure |
| **Maintenance mode** | Pause processing during DDL changes |
| **Compaction trigger** | Lambda to optimize small files |
| **Error replay** | Re-process records from errors table |
| **DLQ replay** | Re-inject archived DLQ messages |

---

## Files Reference

### Core Processing
| Component | Path |
|-----------|------|
| SQS Processor | `modules/compute/lambda/sqs_processor/handler.py` |
| Firehose Transform | `modules/compute/lambda/firehose_transform/handler.py` |
| Standardized Job | `scripts/glue/standardized_processor.py` |
| Curated Job | `scripts/glue/curated_processor.py` |
| Schema Evolution | `scripts/glue/utils/schema_evolution.py` |

### Schema Management
| Component | Path |
|-----------|------|
| Schema Validator | `modules/compute/lambda/schema_validator/handler.py` |
| Ensure Standardized | `scripts/lambda/ensure_standardized_table.py` |
| Schema Files | `schemas/{topic}/active/schema.json` |

### Terraform
| Module | Resources |
|--------|-----------|
| `modules/compute` | Lambdas, Glue jobs |
| `modules/state` | DynamoDB (checkpoints, schema-registry) |
| `modules/orchestration` | Step Functions, EventBridge |
| `modules/catalog` | Glue databases, Iceberg tables |

---

## Key Design Principles

1. **Serverless-first**: Lambda + EventBridge for all management flows
2. **Schema-driven DDL**: Tables created from schema files, not Terraform
3. **Graceful degradation**: Missing schema → skip, not fail
4. **Audit everything**: drift_log, errors table, DLQ archive
5. **Idempotent operations**: Safe to retry any step
