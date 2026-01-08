# Lean-Ops

Simplified, fail-aware data pipeline with 2 layers (RAW → Semantic).

## Architecture

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         GOVERNANCE PLANE                                   │
│  Topic Registry | Schema Registry | PII Masking                            │
├────────────────────────────────────────────────────────────────────────────┤
│                           QUALITY PLANE                                    │
│  Water Balance | Freshness SLA | CDE Validation | Schema Drift             │
├────────────────────────────────────────────────────────────────────────────┤
│                            DATA PLANE                                      │
│  SQS → Lambda → Firehose → RAW (Iceberg) → Semantic Job → Semantic Table   │
└────────────────────────────────────────────────────────────────────────────┘
```

## Components

| Module | Purpose |
|--------|---------|
| `modules/ingestion` | SQS, Lambda, Firehose, DLQ |
| `modules/semantic` | Glue job, Step Functions |
| `modules/shared-services` | DynamoDB tables |
| `modules/quality` | Water Balance, Freshness SLA |

## Fail-Aware Patterns

| Pattern | Implementation |
|---------|----------------|
| Error classification | MALFORMED_JSON → DLQ (no retry) |
| MaximumConcurrency | Limits ESM polling, not Lambda |
| No-data short-circuit | Skip Glue if no new snapshots |
| Duplicate prevention | DynamoDB locks |
| Centralized DLQ | All topics → one DLQ with context |

## Deploy

```bash
# 1. Configure
cp environments/dev.tfvars environments/your-env.tfvars
# Edit with your values

# 2. Initialize
terraform init

# 3. Plan
terraform plan -var-file="environments/your-env.tfvars"

# 4. Apply
terraform apply -var-file="environments/your-env.tfvars"

# 5. Upload Glue script
aws s3 cp modules/semantic/semantic_job.py \
    s3://YOUR_BUCKET/glue-scripts/semantic_job.py
```

## DynamoDB Tables

| Table | Purpose |
|-------|---------|
| `checkpoints` | Last processed snapshot per topic |
| `error_tracker` | All errors with context |
| `counters` | Daily metrics per topic |
| `topic_registry` | Configuration per topic |
| `compaction_tracking` | Compaction history |
| `locks` | Duplicate trigger prevention |

## Folder Structure

```
lean-ops/
├── main.tf                    # Root module
├── environments/
│   └── dev.tfvars            # Environment config
├── modules/
│   ├── ingestion/            # SQS, Lambda, Firehose
│   ├── semantic/             # Glue, Step Functions
│   ├── shared-services/      # DynamoDB tables
│   └── quality/              # Water Balance, Freshness
└── lambda/
    ├── sqs_processor/        # SQS → Firehose
    ├── firehose_transform/   # Dynamic routing
    ├── water_balance/        # RAW ≈ Semantic check
    └── freshness_sla/        # Data freshness check
```
