# Lean-Ops Operations Plan

## 1. Pipeline Architecture

### Data Flow (Sequential Layers)

```
IoT Devices → SQS → Lambda (sqs_processor) → Firehose → Iceberg RAW
                                                           │
                                                           ▼
                                              ┌─────────────────────────┐
                                              │    Step Function        │
                                              │    (Orchestration)      │
                                              └─────────────────────────┘
                                                           │
                                                           ▼
                                              ┌─────────────────────────┐
                                              │    Iceberg Curated      │  ← MERGE from RAW
                                              │    (Auto-Evolve)        │    (Data Scientists: Athena)
                                              └─────────────────────────┘
                                                           │
                                                           ▼
                                              ┌─────────────────────────┐
                                              │    Iceberg Semantic     │  ← MERGE from Curated
                                              │    (Strict Schema)      │    (Analysts: Snowflake)
                                              └─────────────────────────┘
```

**Key:** RAW → Curated → Semantic is a **sequential dependency chain**, not parallel.

---

## 2. Step Function Orchestration (Optimized)

### Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Single Lookup** | `GetAllCheckpoints` returns RAW, Curated, Semantic snapshots + schema_exists in one call |
| **Token Passing** | `RunCurationJob` outputs `curated_snapshot_id`, passed directly to `RunSemanticJob` |
| **Backlog Loop** | If `records_written > 10000`, loop back to `Start` immediately |
| **Drift Notification** | Missing schema triggers CloudWatch metric + DynamoDB `drift_log` entry |

### Flow Diagram

```
                                    Start
                                      │
                                      ▼
                         ┌────────────────────────┐
                         │   GetAllCheckpoints    │  ← Returns: raw_snapshot,
                         └────────────────────────┘    curated_checkpoint,
                                      │                semantic_checkpoint,
                                      │                schema_exists
                                      ▼
                         ┌────────────────────────┐
                         │   CheckForNewRawData   │
                         └────────────────────────┘
                              │              │
                         No   │              │ Yes
                              ▼              ▼
                         NoNewData    ┌────────────────────────┐
                                      │    RunCurationJob      │
                                      └────────────────────────┘
                                                   │
                                                   │ Output: curated_snapshot_id
                                                   ▼
                         ┌────────────────────────┐
                         │ UpdateCurationCheckpoint│
                         └────────────────────────┘
                                      │
                                      ▼
                         ┌────────────────────────┐
                         │   CheckSchemaExists    │
                         └────────────────────────┘
                              │              │
                         No   │              │ Yes
                              ▼              ▼
              ┌─────────────────────┐  ┌────────────────────────┐
              │NotifyMissingSchema  │  │ CheckForNewCuratedData │
              └─────────────────────┘  └────────────────────────┘
                        │                     │              │
                        ▼                No   │              │ Yes
              CurationOnlySuccess             ▼              ▼
                                      NoNewCuratedData ┌────────────────────────┐
                                                       │   RunSemanticJob       │
                                                       └────────────────────────┘
                                                                  │
                                                                  ▼
                                                       ┌────────────────────────┐
                                                       │UpdateSemanticCheckpoint│
                                                       └────────────────────────┘
                                                                  │
                                                                  ▼
                                                       ┌────────────────────────┐
                                                       │  CheckBacklogThreshold │
                                                       └────────────────────────┘
                                                            │              │
                                                       No   │              │ Yes
                                                            ▼              ▼
                                                   FullPipelineSuccess   Loop → Start
```

---

## 3. Partitioning Strategy

| Layer | Partitioning | Bloom Filters | Write Mode |
|-------|--------------|---------------|------------|
| **RAW** | `day(publish_time)` | ❌ | APPEND |
| **Curated** | `period_reference`, `day(publish_time)`, `bucket(16, idempotency_key)` | ✅ | MERGE |
| **Semantic** | `period_reference`, `day(publish_time)`, `bucket(16, idempotency_key)` | ✅ | MERGE |

---

## 4. Code Quality Standards

### Lambda Handler Guidelines

| Rule | Rationale |
|------|-----------|
| Keep handlers < 200 lines | Easier to test and debug |
| Move utilities to `modules/compute/lambda/common/` | Reuse S3, DynamoDB, logging helpers |
| Use structured JSON logging | Enables CloudWatch Insights queries |
| Return `batchItemFailures` for SQS triggers | Enables partial batch retry |

### Terraform Standards

| Standard | Implementation |
|----------|----------------|
| **Naming** | `<project>-<env>-<component>` (e.g., `lean-ops-dev-sqs-processor`) |
| **Provider Locking** | Commit `terraform.lock.hcl`, pin versions in `required_providers` |
| **Remote State** | S3 bucket + DynamoDB lock table |
| **IAM Least-Privilege** | Scope policies to exact ARNs, not `*` |

---

## 5. Repository Hygiene

### Files to Delete

| Path | Action |
|------|--------|
| `.terraform/` | Delete, add to `.gitignore` |
| `tfplan` | Delete, add to `.gitignore` |
| `terraform.tfstate*` | Delete, add to `.gitignore` |
| `modules/compute/.build/` | Delete, add to `.gitignore` |
| `_backup/` | Delete |

### Files to Move/Archive

| Path | Action |
|------|--------|
| `iam-policy-lean-ops.json` | Move to `infra/policies/` or delete |
| `modules/semantic/` | Archive to `archive/` (unused) |
| `modules/unified/` | Archive to `archive/` (unused) |

### .gitignore Additions

```gitignore
# Terraform
.terraform/
*.tfstate
*.tfstate.backup
tfplan
*.tfplan

# Build artifacts
modules/compute/.build/
*.zip

# Backup files
_backup/
```

---

## 6. CI/CD Pipeline (GitHub Actions)

### Workflow Stages

```yaml
name: Lean-Ops CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Terraform Format
        run: terraform fmt -check -recursive
      - name: Terraform Validate
        run: terraform init -backend=false && terraform validate
      - name: TFLint
        uses: terraform-linters/setup-tflint@v4
        with:
          tflint_version: latest

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install Dependencies
        run: pip install pytest boto3 moto
      - name: Run Unit Tests
        run: pytest tests/ -v

  security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Checkov Scan
        uses: bridgecrewio/checkov-action@v12
        with:
          directory: .
          framework: terraform
      - name: Bandit Scan
        run: |
          pip install bandit
          bandit -r modules/compute/lambda/ -ll

  deploy:
    needs: [lint, test, security]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: production
    steps:
      - uses: actions/checkout@v4
      - name: Configure AWS
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Terraform Apply
        run: |
          terraform init
          terraform apply -var-file="environments/prod.tfvars" -auto-approve
```

---

## 7. Monitoring & Alerts

### CloudWatch Alarms

| Alarm | Metric | Threshold | Action |
|-------|--------|-----------|--------|
| DLQ Age | `ApproximateAgeOfOldestMessage` | > 7 days | SNS → PagerDuty |
| Lambda Error Rate | `Errors / Invocations` | > 5% | SNS → Circuit Breaker |
| Step Function Failed | `ExecutionsFailed` | > 0 | SNS → Slack |
| Schema Drift | DynamoDB `drift_log` new items | Any | SNS → Governance Team |

### Dashboard Panels

1. **Ingestion Throughput** – SQS messages/sec, Firehose records/sec
2. **DLQ Status** – Message count, oldest message age
3. **Step Function Health** – Success/Fail/Running executions
4. **Snowflake Query Latency** – P50/P95 for Semantic table queries

---

## 8. Runbook: Common Operations

### Trigger Manual Pipeline Run

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT:stateMachine:lean-ops-pipeline \
  --input '{"force": true}'
```

### Refresh Snowflake External Table

```sql
ALTER EXTERNAL ICEBERG TABLE lean_ops.semantic_events REFRESH;
```

### Replay DLQ Messages

```bash
aws sqs receive-message --queue-url $DLQ_URL --max-number-of-messages 10 | \
  jq -r '.Messages[].Body' | \
  xargs -I {} aws sqs send-message --queue-url $SOURCE_QUEUE_URL --message-body '{}'
```

### Check Pipeline Checkpoints

```bash
aws dynamodb scan --table-name lean-ops-checkpoints --output table
```

---

*Last Updated: 2026-01-11*
