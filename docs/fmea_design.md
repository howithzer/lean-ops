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

---

## 2. Stage: Standardized Layer (Firehose -> Iceberg Raw -> Standardized)

### Failure Mode 2.1: Schema Mismatch (Recoverable - Logic/Schema)
*   **Description:** Incoming JSON is valid but has a new field or data type mismatch (e.g., `string` instead of `int`) that violates the rigorous Iceberg schema.
*   **Detection:** Spark Job fails during `writeTo` or validation step.
*   **System Action:**
    *   Job marked as FAILED.
    *   **Circuit Breaker TRIPS (Status=RED)** to stop processing subsequent batches.
*   **Recovery Strategy:** **Code/Schema Fix + Replay.**
    *   **Diagnosis:** Support team checks the error logs/branch.
    *   **Fix:**
        *   *Scenario A (Evolution):* Update Iceberg Table Schema (`ALTER TABLE ... ADD COLUMN`).
        *   *Scenario B (Bug):* Update Glue Script to handle the type casting.
    *   **Action:** Reset Circuit Breaker (`Status=GREEN`) and Re-drive the Batch.

### Failure Mode 2.2: Data Quality / CDE Violation (Hard Failure - Contextual)
*   **Description:** Data is valid format, but violates business rules (e.g., `idempotency_key` is NULL, `event_timestamp` is missing).
*   **Detection:** Validation Logic in Glue Job (Audit Step).
*   **System Action:**
    *   Row is tagged as `ERROR`.
    *   Batch fails Audit -> **Circuit Breaker TRIPS**.
*   **Recovery Strategy:** **Surgeon / Patch.**
    *   **Diagnosis:** Identify specific bad rows in the Audit Branch.
    *   **Fix:**
        *   *Option A (Strict):* Source system must resend valid data.
        *   *Option B (Patch):* Support team uses `data-patcher` to manually correct the missing CDE (if inferable) and re-inject to SQS.
        *   *Option C (Discard):* If deemed "Hard Failure", move file to "Rejected" bucket, delete row from branch, and commit the rest.

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

To achieve **100% compliance** and zero-touch failures, we utilize the WAP pattern supported by **Iceberg V2** on AWS Glue/EMR.

### 4.1 Support Matrix
*   **Platform:** AWS Glue 4.0+ / EMR 6.x+ (Spark 3.3+)
*   **Format:** Apache Iceberg V2
*   **Feature:** Branching & Tagging

### 4.2 The Workflow
1.  **Write (Staging):**
    *   Instead of writing to `main`, the job creates a branch: `audit_batch_<id>`.
    *   `df.writeTo("glue_catalog.db.table").option("branch", "audit_batch_<id>").append()`
    *   Data is durable on S3 but invisible to consumers.

2.  **Audit (Validation):**
    *   A validation task runs queries against the branch.
    *   `SELECT count(*) FROM table VERSION AS OF 'audit_batch_<id>' WHERE invalid_flag = true`
    *   **If Bad Count > 0:**
        *   **Trip Circuit Breaker (DynamoDB).**
        *   Do NOT merge.
        *   Trigger PagerDuty/SNS.

3.  **Publish (Commit):**
    *   **If Bad Count == 0:**
    *   Perform a fast-foward merge.
    *   `CALL glue_catalog.system.fast_forward('db.table', 'main', 'audit_batch_<id>')`
    *   Remove branch.

### 4.3 Why this works for us
*   **Isolation:** Bad data never pollutes the production view.
*   **Debuggability:** Support teams can query the failed branch to see *exactly* what broke without parsing raw logs.
*   **Atomicity:** The "Publish" is a metadata swap, ensuring all consumers see the batch appear instantly and completely.

---

## Summary of Recovery Actions

| Failure Category | Examples | Automated Recovery? | Manual Action |
| :--- | :--- | :--- | :--- |
| **System/Network** | API Throttling, S3 503 | **Yes** (Retry Policies) | None (Monitor only) |
| **Hard Data** | Malformed JSON | **No** | DLQ Review -> Discard or Patch |
| **Business Data** | NULL CDEs, Logic Rules | **No** (Circuit Breaker) | Patch Data -> Re-inject SQS |
| **Schema/Logic** | Type Mismatch, Bug | **No** (Circuit Breaker) | Fix Code/Schema -> Reset Breaker -> Re-drive |
