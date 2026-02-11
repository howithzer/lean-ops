# Failure Mode and Effects Analysis (FMEA) & Recovery Strategy

## Overview
This document outlines the failure modes, their impact, and the recovery strategies for the end-to-end data ingestion pipeline (SQS -> Firehose -> Standardized -> Curated). Failures are categorized into **Recoverable** (system/logic issues) and **Hard** (data quality/permanent rejection).

It also details the **Write-Audit-Publish (WAP)** architecture used to ensure 100% data compliance.

---

## 1. Stage: Ingestion (SQS -> Firehose)

### Failure Mode 1.1: Malformed JSON (Hard Failure)
*   **Description:** The payload arriving in SQS is not valid JSON (e.g., truncated, binary garbage).
*   **Detection:** Firehose or Lambda transformation fails to parse.
*   **System Action:**
    *   Automatic retry (standard SQS behavior).
    *   After max retries -> Move to **SQS Dead Letter Queue (DLQ)**.
*   **Recovery Strategy:** **Discard or Manual Inspection.**
    *   Since the payload is fundamentally corrupt, it cannot be processed.
    *   **Action:** Operational alert on DLQ depth. Support team inspects. If confirmed garbage, **Purge**. If recoverable (e.g., bad encoding), manually fix and re-inject via `data-patcher`.

### Failure Mode 1.2: System Limitation / Throttling (Recoverable)
*   **Description:** Kinesis/Firehose limits exceeded.
*   **Detection:** AWS API 429 Errors.
*   **System Action:** Exponential Backoff & Retry (Built-in SDK/Service behavior).
*   **Recovery Strategy:** **Automatic.** No manual intervention needed unless persistent (then scale up).

### Failure Mode 1.3: Regional AWS Service Failure (Critical)
*   **Services:** SQS, Firehose, Lambda (Regional)
*   **Description:** An entire AWS region's control plane for SQS or Firehose goes down.
*   **Resilience:**
    *   **SQS:** Standard queues are Multi-AZ by default. High durability.
    *   **Firehose:** Replicates data across AZs.
    *   **Glue/Lambda:** Serverless, runs in multiple AZs.
*   **Strategy:** **Wait.** AWS regional failures are rare and self-healing. We rely on the inherent Multi-AZ architecture. If a true Region outage occurs, the pipeline pauses until service restoration. No data is lost if it was successfully ack'ed by SQS.

---

## 2. Stage: Standardized Layer (Firehose -> Iceberg Raw -> Standardized)

### Failure Mode 2.1: Schema Mismatch (Recoverable - Logic/Schema)
*   **Description:** Incoming JSON is valid but has a new field or data type mismatch (e.g., `string` instead of `int`) that violates the rigorous Iceberg schema.
*   **Detection:** **Glue Job** fails during `writeTo` or validation step.
*   **System Action:**
    *   Job marked as FAILED.
    *   **Circuit Breaker TRIPS (Status=RED)** to stop processing subsequent batches.
*   **Recovery Strategy:** **Code/Schema Fix + Replay.**
    *   **Offset Handling:**
        *   Since we use **Glue Bookmarks** or **Checkpointing**, the job knows exactly where it left off.
        *   When the job failed, it **did not commit** the offset (bookmark) for that batch.
        *   **On Re-Run:** The job restarts from the *same* offset/checkpoint, processing the *same* files from S3/Firehose.
    *   **Action:**
        1.  Fix code/schema.
        2.  Reset Circuit Breaker (`Status=GREEN`).
        3.  Re-drive the Step Function.

### Failure Mode 2.2: Data Quality / CDE Violation (Hard Failure - Contextual)
*   **Description:** Data is valid format, but violates business rules (e.g., `idempotency_key` is NULL, `event_timestamp` is missing).
*   **Detection:** Validation Logic in Glue Job (Audit Step).
*   **System Action:**
    *   Row is tagged as `ERROR`.
    *   Batch fails Audit -> **Circuit Breaker TRIPS**.
*   **Recovery Strategy:** **Surgeon / Patch.**
    *   **Offset Handling:** Same as above. The batch is treated as "failed," so offsets are not advanced.
    *   **Fix:**
        *   Support team uses `data-patcher` to fix specific bad files in place (or re-inject).
    *   **Re-Drive:** Re-running the job processes the *now fixed* data.

---

## 3. Stage: Curated Layer (Standardized -> Curated)

### Failure Mode 3.1: Transformation Checkpoint Failure (Recoverable)
*   **Description:** The job reading from Standardized to Curated crashes (e.g., OOM, underlying S3 error).
*   **Detection:** Step Function failure.
*   **System Action:** Job fails. No data committed (Atomic transaction).
*   **Recovery Strategy:** **Retry.**
    *   Since the source execution (Standardized) was successful, this is a pure processing failure.
    *   **Action:** Restart the Step Function from the Curated step.

---

## 4. Architecture Pattern: Write-Audit-Publish (WAP)

To achieve **100% compliance** and zero-touch failures, we utilize the WAP pattern supported by **Iceberg V2** on **AWS Glue**.

### 4.1 Support Matrix
*   **Compute:** AWS Glue 4.0+ (Spark 3.3+). **No EMR required.**
*   **Storage:** S3 (Iceberg V2 Format).
*   **Feature:** Branching & Tagging.

### 4.2 The Happy Path Workflow
1.  **Write (Staging):** Glue Job creates a branch `audit_batch_<id>` and writes results there.
    *   *Note on Offsets:* The job reads input files based on the last successful bookmark.
2.  **Audit (Validation):**
    *   **Execution:** A lightweight **Glue Task** (or python shell job) runs immediately after the write.
    *   **Logic:** `SELECT count(*) FROM table VERSION AS OF 'audit_batch_<id>' WHERE invalid_flag = true`
3.  **Publish (Commit):** If validation passes, performing a fast-forward merge to `main`.
    *   **Commit Offset:** Only *after* the WAP commit succeeds do we mark the Job Bookmark as "processed."

### 4.3 The Recovery Logic (Abandon & Retry)
When a batch fails the Audit step, we do NOT try to "fix" the branch. We **abandon** it and **retry** the process from the start.

1.  **Branch Freeze:** The failed branch `audit_batch_failed_1` is left untouched using `retention` settings.
2.  **Circuit Breaker:** Logic stops.
3.  **Remediation:** Fix data/code.
4.  **Re-Run:**
    *   Restart the Glue Job.
    *   Because the bookmark was never advanced, Glue **re-reads the exact same input files**.
    *   It creates a **NEW** branch: `audit_batch_retry_1`.
5.  **Commit:** `audit_batch_retry_1` -> `main`. Old branch is ignored/deleted by cleanup policy.

### 4.4 Deep Dive: Loop Prevention via "Quarantine Pattern"
**Problem:** We cannot modify Raw data (Immutable), but re-running the job with the same offset will pick up the same bad record.

**Solution:** We use a **Quarantine Table (DynamoDB)** to virtually "mask" bad records during processing.

1.  **The Failure:** Job fails on Record X (ID: `GCP_123`). Circuit Breaker trips.
2.  **The Fix (Quarantine):**
    *   Support adds `GCP_123` to `QuarantineTable`.
    *   *Raw Data is untouched.*
3.  **The Re-Run (Glue Logic):**
    *   Job starts. Reads offsets N..M.
    *   **Filtering Step:**
        *   `df_clean = df_raw.filter(NOT (id IN Quarantine AND is_replay == false))`
        *   *Meaning:* Block the ID *unless* it's marked as a fixed replay.
    *   **Result:** The original bad record (no flag) is skipped. The batch passes.
4.  **Re-Injection (The Patch):**
    *   Support uses `data-patcher` to fix payload and re-inject.
    *   **Crucial:** The Patcher adds `is_replay: true` to the body/metadata.
    *   This new record (`GCP_123`, `is_replay=true`) lands in a *future* batch.
    *   Because of the flag, it **Bypasses** the quarantine filter and is processed successfully.

**Why this is safer:**
*   **Immutability:** Raw data is never deleted.
*   **Idempotency:** We keep the original Business ID (`GCP_123`).
*   **loop-prevention:** The filter is smart enough to block the bad copy and allow the good copy.

---

## 5. Tool Walkthrough: The Data Patcher

The `data-patcher` is a specific CLI tool for the support team.

### 5.1 Technology Specs
*   **Language:** Python 3.9+ (Boto3).
*   **Environment:** Runs on a dedicated **EC2 Bastion Host** or admin's local machine (with IAM roles). Can also be deployed as a **Lambda Function** for automated fixes.
*   **Permissions:** Needs S3 Read/Write and SQS SendMessage.

### 5.2 Scenario & Workflow
A batch failed because one file contained a special character.

**1. Inspect (Diagnosis)**
```bash
$ data-patcher inspect s3://bucket/path/to/bad_file.json
> Found unprintable character '\x1F' at line 1402.
```

**2. Sanitize and Re-Wrap (The Fix)**
Crucially, the tool **extracts the original metadata** (`messageId`, `idempotency_key`) before creating the new payload.
```bash
$ data-patcher secure-clean s3://bucket/path/to/bad_file.json --remove-chars "\x1F" --re-inject-queue production-queue
> Injecting to SQS with attributes:
>   - is_replay: true
>   - original_message_id: 550e8400...
>   - idempotency_key: [preserved]
```

**3. Resume (The Re-Drive)**
```bash
$ data-patcher resume-pipeline --pipeline standard_ingest
> Circuit Breaker Reset (Status=GREEN). Triggering Step Function.
```

---

## Summary of Recovery Actions

| Failure Category | Examples | Automated Recovery? | Manual Action |
| :--- | :--- | :--- | :--- |
| **System/Network** | API Throttling, S3 503 | **Yes** (Retry Policies) | None (Monitor only) |
| **Hard Data** | Malformed JSON | **No** | DLQ Review -> Discard or Patch |
| **Business Data** | NULL CDEs, Logic Rules | **No** (Circuit Breaker) | Patch Data -> Re-inject SQS |
| **Schema/Logic** | Type Mismatch, Bug | **No** (Circuit Breaker) | Fix Code/Schema -> Reset Breaker -> Re-drive |
