# Architecture Feedback & Cleanup Recommendations

## Overview
The **lean‑ops** repository implements a modular, fail‑aware data pipeline for ingesting IoT events and persisting them in Apache Iceberg tables.  The current codebase is functional, but there are several areas where the architecture, documentation, and repository hygiene can be improved.

---

## 1. High‑Level Architecture Review

| Layer | Current Implementation | Suggested Improvements |
|-------|-----------------------|------------------------|
| **Ingestion** | SQS per topic → Lambda (`sqs_processor`) → Kinesis Firehose → Iceberg RAW | • Add a thin **ingestion façade** (e.g., API Gateway) for devices that cannot push directly to SQS.<br>• Consider using **Kinesis Data Streams** for lower latency if required. |
| **Resilience** | DLQ → `dlq_processor` Lambda → S3 archive + DynamoDB error tracker. Circuit‑breaker Lambda disables the event source mapping on high error rates. | • Move the **DLQ processor** into its own Terraform module for clearer separation.<br>• Add **DLQ age alarm** (7‑day) – already in the roadmap – to catch stale messages early.<br>• Store a **replay token** (original SQS message ID) in DynamoDB to enable automated replay scripts. |
| **Storage** | Iceberg RAW tables on S3, curated tables via Glue jobs. | • Use **Iceberg table snapshots** for time‑travel queries.<br>• Add a **metadata catalog** (Glue) versioning policy to enforce schema evolution rules. |
| **Curation** | Step Functions orchestrate Glue jobs that read from RAW and write to curated tables. | • Introduce a **pipeline manifest** (e.g., a DynamoDB entry) that records each curation run – useful for audit and rollback. |
| **Observability** | CloudWatch metric alarm on Lambda error rate, SNS alerts, DynamoDB error tracker. | • Add **structured logs** (JSON) for all Lambdas and ship them to a centralized log store (e.g., Elasticsearch/OpenSearch).<br>• Create a **dashboard** that visualises ingestion lag, DLQ size, and error‑type breakdown. |

---

## 2. Documentation Gaps

1. **README** – now focuses on the IoT use‑case, but it still mixes high‑level description with deployment steps. Split the README into two sections:
   - **Project Overview** (architecture diagram, data flow, key concepts).
   - **Getting Started** (prerequisites, quick‑start commands, testing).
2. **Lessons Learned** – good content, but it should be versioned per wave (e.g., `LESSONS_LEARNED_WAVE1.md`). This makes it easier to track progress.
3. **Testing** – there is no explicit test harness for the Lambda functions. Add a `tests/` directory with unit tests (pytest) and integration tests (localstack or SAM CLI).
4. **Contribution Guidelines** – a `CONTRIBUTING.md` would help external contributors understand the module layout and CI pipeline.

---

## 3. Code Quality & Maintainability

- **Lambda Handlers** – keep each handler small and move reusable utilities (e.g., S3 upload, DynamoDB put) into a shared library (`modules/compute/lambda/common/`).
- **Terraform Modules** – the current modules are well‑scoped, but a few unused modules (`semantic`, `unified`) remain in the repo. Remove them or archive them in a separate `archive/` folder.
- **Provider Locking** – the `terraform.lock.hcl` file should be committed (it already is) and the provider versions pinned in `required_providers`.
- **State Backend** – configure a remote backend (S3 + DynamoDB lock) to avoid local state drift.
- **Naming Conventions** – enforce a consistent naming pattern for resources: `<project>-<env>-<component>` (already used, but ensure all resources follow it, including IAM roles and policies).
- **IAM Least‑Privilege** – tighten policies for the Lambda roles; they currently have broad `*` permissions for S3 and DynamoDB. Scope them to the exact bucket/table ARNs.

---

## 4. Repository Clean‑up

| Path | Action |
|------|--------|
| `.terraform/` | Delete – generated local state. Add to `.gitignore`.
| `.terraform.lock.hcl` | Keep – locks provider versions.
| `tfplan` | Delete – temporary plan file. Add to `.gitignore`.
| `iam-policy-lean-ops.json` | Move to `infra/policies/` if needed, otherwise delete.
| `modules/semantic/` and `modules/unified/` | Remove (unused) or archive under `archive/`.
| `modules/compute/.build/` (zip artifacts) | Delete – these are build artefacts. Add `modules/compute/.build/` to `.gitignore`.
| `_backup/` | Delete – old backup files.
| `terraform.tfstate` & `terraform.tfstate.backup` | Delete – never commit state files. Ensure `.gitignore` contains them.
| `README.md` – already updated to IoT focus.
| `docs/` – keep only `LESSONS_LEARNED.md` and move other docs to `docs/` if they are still relevant.

---

## 5. Suggested CI/CD Pipeline (GitHub Actions)

1. **Lint** – `terraform fmt -check`, `terraform validate`, `tflint`.
2. **Unit Tests** – run `pytest` for Lambda code.
3. **Integration Tests** – spin up a localstack environment, apply Terraform, run a small data‑injector test.
4. **Security Scan** – `checkov` or `tfsec` for Terraform, `bandit` for Python.
5. **Deploy** – on merge to `main`, run `terraform apply` with a manual approval step.

---

## 6. Next Steps (Short‑Term)

- Create the `architecture_feedback` folder and add this advice file.
- Remove the dead modules and artefact directories as listed.
- Add a minimal CI workflow (lint + unit tests) to the repository.
- Draft a `CONTRIBUTING.md` that references the updated README and the new CI pipeline.
- Implement the DLQ age alarm (7‑day) and add it to the `observability` module.

---

*Prepared for the user on 2026‑01‑10.*
