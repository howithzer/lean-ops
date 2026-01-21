# Lean-Ops Data Pipeline Architecture

> **For Developers** | Last Updated: 2026-01-21

---

## What Does This Pipeline Do?

Think of it like a mail sorting facility:

1. **Mail arrives** (SQS) → **Clerk opens it** (Lambda) → **Puts in truck** (Firehose) → **Warehouse** (RAW)
2. **Sorter organizes mail** (Standardized Job) → **Inspector checks quality** (Curated Job) → **Ready for delivery** (Athena/Snowflake)

---

## The Journey of Your Data

```
Your App → SQS → Lambda → Firehose → RAW Table → Standardized Table → Curated Table → Analysts
```

### Step-by-Step

| Step | What Happens | Analogy |
|------|--------------|---------|
| 1. **SQS** | Messages wait in a queue | Mailbox |
| 2. **Lambda** | Adds `topic_name` to each message | Clerk stamps "from Engineering" |
| 3. **Firehose** | Batches messages, writes to S3 as Parquet | Mail truck loads up, drives to warehouse |
| 4. **RAW Table** | Landing zone. No validation. Just store everything. | Warehouse floor - pile of boxes |
| 5. **Standardized Table** | Flatten JSON, remove duplicates | Unpacking boxes, sorting by type |
| 6. **Curated Table** | Validate required fields, convert types | Quality check before shipping |

---

## Why Do We Deduplicate Twice?

Different problems need different solutions:

| Problem | Solution | Layer |
|---------|----------|-------|
| Lambda retried and sent the same message twice | Keep the **first** one (FIFO) | Standardized |
| App sent a correction ("amount was wrong, here's the fix") | Keep the **latest** one (LIFO) | Curated |

---

## What Is a "Schema Gate"?

Before processing runs, we check if a schema file exists in S3:

- **No schema?** → Skip this topic. It's not ready.
- **Schema exists?** → Process it.

This lets you onboard new topics safely. Just drop a `{topic}.json` file when ready.

---

## Where Do Errors Go?

| What Went Wrong | Where It Goes | How to Find It |
|-----------------|---------------|----------------|
| Lambda crashed | SQS Dead Letter Queue → `iceberg_raw_db.dlq_errors` | Query the table |
| JSON was malformed (empty, cut off) | `iceberg_standardized_db.parse_errors` | Query the table |
| Required field was missing | `iceberg_curated_db.errors` | Query the table |
| Firehose couldn't write | S3 `firehose-errors/` prefix | Check S3 bucket |

---

## How Do I Deploy?

### Prerequisites
- AWS CLI configured with profile `terraform-firehose`
- Terraform installed

### Commands

```bash
# 1. Build Lambda packages
./scripts/build_lambdas.sh

# 2. Build Glue package
./scripts/build_glue.sh

# 3. Deploy infrastructure
AWS_PROFILE=terraform-firehose terraform apply -var-file="environments/dev.tfvars"

# 4. Upload schema (this "opens the gate")
./tests/e2e/production_emulation.sh schema

# 5. Run a test
./tests/e2e/production_emulation.sh full

# 6. Tear down when done
AWS_PROFILE=terraform-firehose terraform destroy -var-file="environments/dev.tfvars"
```

---

## Where Is Everything?

| What | File Location |
|------|---------------|
| SQS Processor Lambda | `modules/compute/lambda/sqs_processor/handler.py` |
| Firehose Transform Lambda | `modules/compute/lambda/firehose_transform/handler.py` |
| Standardized Glue Job | `scripts/glue/standardized_processor.py` |
| Curated Glue Job | `scripts/glue/curated_processor.py` |
| Step Functions (orchestration) | `modules/orchestration/main.tf` |
| Table definitions | `modules/catalog/main.tf` |
| Schema files | `schemas/events.json`, `schemas/curated_schema.json` |

---

## FAQ

**Q: How often does processing run?**  
A: Every 15 minutes (EventBridge triggers Step Functions).

**Q: What if a Glue job fails?**  
A: Step Functions sends an SNS alert. The next run will retry from where it left off (checkpointed in DynamoDB).

**Q: Can I add a new topic?**  
A: Yes. Create the SQS queue, add it to `topics` in Terraform, and drop a schema file in S3.

---

## Not Yet Implemented

| Feature | Status |
|---------|--------|
| Drift Log (tracks schema changes) | Table exists, not populated |
| DLQ retry mechanism | Messages archived, no replay |
| Semantic Layer | Not built |
