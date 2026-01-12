# Lean-Ops: Release Notes

## Current Release: v1.2.0 (Wave 3)

**Date: 2026-01-11**

### Features

#### Common Services Library
- Extracted shared utilities from Lambda handlers into `lambda/common/`
- `topic_utils.py`: Extract topic names from SQS ARNs
- `error_classification.py`: DROP/RETRY logic with `ErrorAction` enum
- `aws_clients.py`: Lazy boto3 client factories with caching
- `checkpoint_utils.py`: Iceberg snapshot + DynamoDB checkpoint queries

#### GetAllCheckpoints Lambda
- Single Lambda for Step Function optimization
- Returns RAW/Curated/Semantic snapshots in one call
- Includes `schema_exists` flag for governance checks
- Calculates `has_new_raw_data` and `has_new_curated_data`

#### Build Script
- `scripts/build_lambdas.sh` packages Lambdas with common library
- Run before `terraform apply`

---

## Previous Release: v1.1.0 (Wave 2)

**Date: 2026-01-10**

### Features

#### DLQ Processor Lambda
- Archives failed messages to S3: `dlq-archive/{topic}/{date}/{message_id}.json`
- Logs to DynamoDB `error_tracker` table
- Enables message replay

#### Error Classification
- `MALFORMED_JSON` → DROP (log and return success)
- `FIREHOSE_THROTTLE` → RETRY (transient)
- `UNKNOWN` → RETRY → DLQ after `maxReceiveCount`

#### Circuit Breaker
- CloudWatch alarm on error rate > 50%
- SNS → Lambda to disable/enable Event Source Mappings
- Prevents cascading failures

---

## Baseline Release: v1.0.0 (Wave 1)

**Date: 2026-01-09**

### Features

#### SQS Processor Lambda
- Multi-topic support with topic extraction from queue ARN
- `ReportBatchItemFailures` for proper error handling
- Scaling config with max concurrency = 50

#### Firehose Transform Lambda
- Dynamic Iceberg table routing via `otfMetadata`
- Routes by `topic_name` to `{topic}_staging` tables

#### Infrastructure
- Modular Terraform (8 modules)
- Multi-topic SQS queues with centralized DLQ
- CloudWatch alarms for DLQ and error rate

---

## Deployment Commands

```bash
# Build Lambda packages with common library
./scripts/build_lambdas.sh

# Deploy
AWS_PROFILE=terraform-firehose terraform apply -var-file="environments/dev.tfvars"
```

---

## Architecture Overview

```
EKS Pods
    ↓
SQS Queues (per topic)
    ↓
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
├─────────────────────────────────────────────────────────────────┤
│  SQS Processor Lambda                                            │
│  ├── Extract topic from queue ARN                               │
│  ├── Classify errors (DROP/RETRY)                               │
│  └── Send to Firehose                                           │
├─────────────────────────────────────────────────────────────────┤
│  Firehose + Transform Lambda                                     │
│  ├── Add otfMetadata for routing                                │
│  └── Write to Iceberg RAW tables                                │
└─────────────────────────────────────────────────────────────────┘
    ↓
RAW Iceberg Tables (iceberg_raw_db.{topic}_staging)
    ↓
┌─────────────────────────────────────────────────────────────────┐
│                    CURATION LAYER (Step Function)               │
├─────────────────────────────────────────────────────────────────┤
│  GetAllCheckpoints Lambda → Glue Job → Update Checkpoint        │
│  ├── Curated: Auto-evolving schema, ALL STRING                  │
│  └── Semantic: Governed schema, TYPED columns                   │
└─────────────────────────────────────────────────────────────────┘
    ↓
Curated/Semantic Iceberg Tables
    ↓
Snowflake (External Managed Iceberg Table)
```
