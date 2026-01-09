# Data Layer Definitions

## Executive Summary

| Layer | Definition | Key Characteristic |
|-------|------------|-------------------|
| **RAW** | What we received | Immutable record of truth |
| **Curated** | What we have | Deduplicated, flattened, flexible |
| **Semantic** | What it means | Typed, validated, governed |

---

## Data Flow Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         SOURCE SYSTEMS                                     │
│            (EKS, GCP Pub/Sub, Kafka, External APIs)                        │
└────────────────────────────┬───────────────────────────────────────────────┘
                             │
                             ▼
┌────────────────────────────────────────────────────────────────────────────┐
│   RAW LAYER (Bronze)                                                       │
│   "What we received"                                                       │
│   ─────────────────────────────────────────────────────────────────────    │
│   • Append-only, immutable                                                 │
│   • Full JSON payload preserved                                            │
│   • Network duplicates present                                             │
│   • Compliance & audit trail                                               │
└────────────────────────────┬───────────────────────────────────────────────┘
                             │
        ┌────────────────────┴────────────────────┐
        │                                          │
        ▼                                          ▼
┌─────────────────────────────┐     ┌─────────────────────────────┐
│  CURATED LAYER (Silver)     │     │  SEMANTIC LAYER (Gold)      │
│  "What we have"             │     │  "What it means"            │
│  ─────────────────────────  │     │  ─────────────────────────  │
│  • Deduplicated             │     │  • Deduplicated             │
│  • Flattened JSON           │     │  • Schema-governed          │
│  • ALL STRING types         │     │  • TYPED columns            │
│  • Auto-evolving schema     │     │  • CDE validated            │
│  • Fast access to new fields│     │  • PII masked               │
└─────────────────────────────┘     └─────────────────────────────┘
        │                                          │
        ▼                                          ▼
   Data Scientists                           BI / Analysts
   Exploration                               Production Reports
   Prototyping                               Dashboards
```

---

## RAW Layer (Bronze)

### Definition

> The **RAW layer** is the immutable, append-only staging area that captures every message exactly as it was received from source systems. It serves as the authoritative record of all ingested data, preserving the complete transaction history for compliance, debugging, and reprocessing.

### Purpose

- **Compliance**: Maintain an unaltered record of all transactions received
- **Debugging**: Investigate pipeline issues by examining original payloads
- **Reprocessing**: Rebuild downstream layers from source of truth if needed
- **Lineage**: Trace any record back to its original form

### Characteristics

| Attribute | Value |
|-----------|-------|
| **Mutability** | Append-only (never updated or deleted) |
| **Schema** | Schema-agnostic (`json_payload` column) |
| **Deduplication** | None (duplicates from retries preserved) |
| **Data Types** | Minimal: `message_id`, `ingestion_ts`, `json_payload` |
| **Partitioning** | By ingestion day (`day(publish_time)`) |
| **Storage Format** | Parquet via Iceberg |
| **Retention** | Long-term (regulatory compliance period) |

### Transactional Context

For transactional data (payments, orders, account changes):
- **Network retries** from EKS/GCP appear as duplicate `message_id` values
- **Application corrections** appear as separate records with same `idempotency_key`
- Both are preserved in RAW; deduplication happens in downstream layers

### Consumers

| User | Use Case |
|------|----------|
| Data Engineer | Debug pipeline failures |
| Compliance Officer | Audit trail for regulators |
| Platform Team | Reprocess historical data |

---

## Curated Layer (Silver)

### Definition

> The **Curated layer** is a deduplicated, flattened representation of the transactional data with automatic schema evolution. All columns are STRING type, enabling immediate access to new fields without schema management overhead. This layer prioritizes **speed of access** over data governance.

### Purpose

- **Exploration**: Enable data scientists to quickly access all available fields
- **Prototyping**: Build and test models without waiting for schema approvals
- **Flexibility**: Automatically incorporate new fields as they appear in source data
- **Low Friction**: Remove schema management bottleneck for exploratory work

### Characteristics

| Attribute | Value |
|-----------|-------|
| **Mutability** | Append with MERGE (upsert on `idempotency_key`) |
| **Schema** | Auto-evolving (new fields added via DDL) |
| **Deduplication** | Two-stage: FIFO (`message_id`) + LIFO (`idempotency_key`) |
| **Data Types** | ALL STRING (cast at query time) |
| **Partitioning** | By transaction day |
| **Storage Format** | Parquet via Iceberg |
| **Schema Drift** | Auto-handled (columns added automatically) |

### Transactional Context

For transactional data:
- **Network retries** are removed (keep first by `message_id`)
- **Application corrections** are applied (keep latest by `idempotency_key`)
- New fields (e.g., `new_fee_amount`) appear automatically as STRING columns
- Users cast to appropriate types at query time: `CAST(amount AS DECIMAL(10,2))`

### Trade-offs

| Benefit | Cost |
|---------|------|
| Immediate access to new fields | No type safety at storage level |
| No schema governance bottleneck | Larger storage (denormalized) |
| Faster turnaround for data scientists | May duplicate semantic data |

### Consumers

| User | Use Case |
|------|----------|
| Data Scientist | Explore new transaction fields |
| ML Engineer | Feature engineering prototypes |
| Analyst (Ad-hoc) | Quick data investigation |

---

## Semantic Layer (Gold)

### Definition

> The **Semantic layer** is the business-ready, schema-governed representation of transactional data. It contains typed columns aligned to a registered schema, with validated Critical Data Elements (CDEs), masked PII, and applied business rules. This layer prioritizes **data quality and governance** for production analytics.

### Purpose

- **Production Analytics**: Trusted data for dashboards and reports
- **Data Quality**: Enforce required fields and data types
- **Governance**: Schema changes require explicit approval process
- **Compliance**: PII fields are masked according to policy
- **Business Rules**: Derived columns and transformations applied

### Characteristics

| Attribute | Value |
|-----------|-------|
| **Mutability** | Append with MERGE (upsert on `idempotency_key`) |
| **Schema** | Strict (defined in schema registry) |
| **Deduplication** | Two-stage: FIFO (`message_id`) + LIFO (`idempotency_key`) |
| **Data Types** | TYPED: STRING, INT, DECIMAL, TIMESTAMP |
| **Partitioning** | By transaction day |
| **CDE Validation** | Required fields enforced (violations → errors table) |
| **PII Masking** | Sensitive fields hashed (SSN, email, etc.) |
| **Schema Drift** | Logged to `drift_log` table; no auto-add |

### Transactional Context

For transactional data:
- Only fields defined in schema are extracted
- `amount` is typed as `DECIMAL(18,2)` not STRING
- `transaction_date` is typed as `TIMESTAMP`
- Missing required fields (CDEs) route record to `errors` table
- New fields in source are logged but NOT added until schema update

### Trade-offs

| Benefit | Cost |
|---------|------|
| Type safety at storage level | New fields require schema update |
| Data quality enforcement | Governance process overhead |
| BI tools work optimally | May lag behind data scientist needs |

### Consumers

| User | Use Case |
|------|----------|
| BI Developer | Build production dashboards |
| Business Analyst | Generate regulatory reports |
| ML Production | Serve features to models |
| API Consumer | Query for downstream systems |

---

## Layer Comparison

| Aspect | RAW | Curated | Semantic |
|--------|-----|---------|----------|
| **Schema** | Agnostic | Auto-evolving | Strict |
| **Types** | Minimal | ALL STRING | Typed |
| **Dedup** | ❌ | ✅ | ✅ |
| **New Fields** | In JSON | Auto-added | Schema update |
| **CDE Validation** | ❌ | ❌ | ✅ |
| **PII Masking** | ❌ | ❌ | ✅ |
| **Query Performance** | Slow (JSON parse) | Fast (flat) | Fast (typed) |
| **Primary Consumer** | Engineers | Data Scientists | Analysts/BI |

---

## Deduplication Strategy (Transactional)

| Stage | Key | Order | Purpose |
|-------|-----|-------|---------|
| **Stage 1** | `message_id` | FIFO (first wins) | Remove network retries |
| **Stage 2** | `idempotency_key` | LIFO (last wins) | Apply business corrections |

**Example**:
```
# Network retry (same message_id) → Keep FIRST
message_id: "abc-123", amount: 100.00  ← KEEP
message_id: "abc-123", amount: 100.00  ← DISCARD

# Business correction (same idempotency_key) → Keep LAST
idempotency_key: "order-789", amount: 100.00  ← DISCARD
idempotency_key: "order-789", amount: 150.00  ← KEEP (correction)
```

---

## Partitioning Strategy

### Multi-Level Partitioning

| Layer | Partition 1 | Partition 2 | Reason |
|-------|-------------|-------------|--------|
| **RAW** | `day(publish_time)` | - | Incremental processing |
| **Curated** | `period_reference` | `day(publish_time)` | Reconciliation + incremental |
| **Semantic** | `period_reference` | `day(publish_time)` | Reconciliation + incremental |

### Why `period_reference` as Primary Partition?

Reconciliation jobs run at **end of each period**, querying all transactions for that period:

```sql
-- Period-end reconciliation query
SELECT SUM(amount), COUNT(*) 
FROM semantic.transactions
WHERE period_reference = '2026-01'
```

| Without `period_reference` partition | With `period_reference` partition |
|--------------------------------------|-----------------------------------|
| Full table scan (12+ months) | Single partition scan |
| Slow, expensive | Fast, cheap |

### Iceberg DDL

```sql
CREATE TABLE semantic.transactions (
    -- columns
)
PARTITIONED BY (
    period_reference,          -- Primary: reconciliation queries
    day(publish_time)          -- Secondary: incremental processing
)
TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'parquet'
);
```

### Query Optimization

| Query Pattern | Partition Used |
|---------------|----------------|
| "Reconcile period 2026-01" | `period_reference` pruning |
| "Process today's ingestion" | `day(publish_time)` pruning |
| "Both" | Both partitions pruned |

---

## Summary

| Layer | One-Liner |
|-------|-----------|
| **RAW** | "What we received" — immutable audit trail |
| **Curated** | "What we have" — flexible exploration layer |
| **Semantic** | "What it means" — governed production layer |
