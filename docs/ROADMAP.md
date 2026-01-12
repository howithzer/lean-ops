# Lean-Ops: Implementation Roadmap

## Current State (Wave 3 Complete)

### ✅ Completed

| Wave | Feature | Status |
|------|---------|--------|
| 1 | SQS Processor Lambda | ✅ Deployed |
| 1 | Firehose Transform Lambda | ✅ Deployed |
| 1 | Multi-topic routing | ✅ Deployed |
| 2 | DLQ Processor Lambda | ✅ Deployed |
| 2 | Error Classification | ✅ Deployed |
| 2 | Circuit Breaker | ✅ Deployed |
| 3 | Common Library | ✅ Deployed |
| 3 | GetAllCheckpoints Lambda | ✅ Ready |
| 3 | Build Script | ✅ Created |

### ⏳ Pending

| Wave | Feature | Priority |
|------|---------|----------|
| 4 | Step Function Orchestration | High |
| 4 | Curated Layer (Glue Job) | High |
| 4 | Semantic Layer (Glue Job) | High |
| 5 | Snowflake Integration | Medium |
| 5 | Lake Formation Permissions | Medium |
| 6 | CI/CD Pipeline | Medium |
| 6 | Production Hardening | Medium |

---

## Wave 4: Step Function Orchestration

### Overview

Optimized Step Function flow with parallel checkpoint lookup and token passing:

```
Start
  │
  ▼
GetAllCheckpoints  ← Single Lambda returns all checkpoints
  │
  ▼
CheckForNewRawData
  │
  ├─ No  → NoNewData (exit)
  │
  └─ Yes → RunCurationJob
              │
              ▼
         CheckSchemaExists
              │
              ├─ No  → NotifyMissingSchema → CurationOnlySuccess
              │
              └─ Yes → RunSemanticJob → CheckBacklogThreshold
                                              │
                                              ├─ No  → FullPipelineSuccess
                                              └─ Yes → Loop → Start
```

### Key Features

1. **Parallel Checkpoint Lookup**: `GetAllCheckpoints` Lambda returns all snapshots in one call
2. **Token Passing**: `curated_snapshot_id` passed from Curation to Semantic job
3. **Backlog Loop**: Automatically catches up during high-volume periods
4. **Drift Notification**: CloudWatch metric + log if Semantic schema missing

---

## Wave 5: Snowflake Integration

### External Managed Iceberg Table (EMIT)

```sql
CREATE EXTERNAL ICEBERG TABLE semantic_db.events
  CATALOG_SYNC = 'LAKEFORMATION'
  EXTERNAL_VOLUME = 'iceberg_volume'
  CATALOG_TABLE_NAME = 'events_semantic';
```

### Schema Refresh Automation

After schema changes, trigger:
```sql
ALTER EXTERNAL ICEBERG TABLE semantic_db.events REFRESH;
```

---

## Wave 6: CI/CD Pipeline

### GitHub Actions Workflow

```yaml
name: lean-ops-ci
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install ruff
      - run: ruff check modules/compute/lambda/

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install pytest moto boto3
      - run: pytest tests/ -v

  plan:
    runs-on: ubuntu-latest
    needs: [lint, test]
    steps:
      - uses: actions/checkout@v4
      - run: ./scripts/build_lambdas.sh
      - run: terraform init
      - run: terraform plan -var-file="environments/dev.tfvars"
```

---

## Production Readiness Checklist

- [x] Error classification (DROP/RETRY)
- [x] DLQ Processor Lambda
- [x] Circuit Breaker
- [x] Common Library extraction
- [x] Build script for Lambda packaging
- [ ] Step Function orchestration
- [ ] Lake Formation permissions
- [ ] VPC deployment
- [ ] KMS encryption
- [ ] Remote Terraform state (S3 + DynamoDB)
- [ ] Least-privilege IAM policies
- [ ] CI/CD pipeline
