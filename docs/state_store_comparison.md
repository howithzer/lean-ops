# Architecture Decision: State Store (DynamoDB vs. RDS)

**Date:** 2026-01-21  
**Context:** Lean-Ops Data Pipeline (Serverless, Event-Driven)  
**Decision Subject:** Storage for Interim State, Checkpoints (High Water Marks), and Distributed Locks.

---

## Executive Summary

For the "Lean-Ops" architecture—which relies heavily on **AWS Lambda** (high concurrency, short-lived compute) and **Event-Driven** patterns—**Amazon DynamoDB** is the strongly recommended choice over RDS PostgreSQL.

**Key Reason:** The "Checkpoint" and "Distributed Lock" patterns are fundamentally **Key-Value** lookups, not relational data models. Using RDS introduces significant complexity (connection pooling, VPC management, scaling overhead) with no functional benefit for this specific use case.

---

## Comparison Matrix

| Feature | DynamoDB (Recommended) | RDS PostgreSQL | Implications for Lean-Ops |
| :--- | :--- | :--- | :--- |
| **Compute Model** | Serverless / On-Demand | Provisioned Instance | **DynamoDB** scales to 0 or 10k IOPS instantly. RDS requires rightsizing and idle cost. |
| **Connection Model** | HTTP (Stateless) | TCP Socket (Persistent) | **Critical:** Lambda scales rapidly. thousands of Lambdas can exhaust RDS connections (max_connections) instantly without **RDS Proxy** ($$$). DynamoDB has no such limit. |
| **Data Model** | Key-Value / Document | Relational | Checkpoints are simple KV pairs: `TopicID -> {Timestamp, SnapshotID}`. Relational integrity is not strictly needed here. |
| **Latency** | Single-digit ms (Predictable) | Variable (depends on load/query) | Checkpoints are read/written on *every* batch. Low latency is crucial for throughput. |
| **Maintenance** | Zero (No patching, no upgrades) | Moderate to High (Vacuuming, upgrades, patching) | Lean-Ops aims for "Low Ops". RDS adds operational burden. |
| **Cost** | Pay-per-request (Great for bursty workloads) | Hourly instance cost (Expensive if idle) | IoT workloads are often bursty. DynamoDB On-Demand fits this curve perfectly. |

---

## Detailed Analysis

### 1. Functional Fit: The "High Water Mark" Pattern
The requirement is to store a bookmark for each topic:
```json
{
  "topic_name": "vehicle_telemetry",
  "partition": "0",
  "last_ingested_ts": 1705881600,
  "last_snapshot_id": 987654321
}
```

*   **DynamoDB:** This is a `GetItem` / `PutItem` operation by Primary Key (`topic_name`). It is O(1) complexity—fast, cheap, and simple.
*   **RDS:** Requires `SELECT * FROM checkpoints WHERE topic = '...'`. While fast, it requires parsing SQL, planning a query, and checking transactional consistency (MVCC) for a simple lookup. It's overkill.

### 2. The "Lambda Problem" (Connection Exhaustion)
*   **Scenario:** A traffic spike causes 500 concurrent Lambda instances to spin up to process SQS messages.
*   **DynamoDB:** Handles 500 concurrent HTTP requests effortlessly.
*   **RDS:** 500 Lambdas try to open 500 TCP connections.
    *   **Result:** `FATAL: remaining connection slots are reserved for non-replication superuser roles`. The DB crashes or rejects connections.
    *   **Fix:** You must deploy **RDS Proxy** (additional infrastructure, additional cost) to manage connection pooling.

### 3. Distributed Locking (for Deduplication/Concurrency)
We currently use a `locks` table to prevent race conditions (e.g., ensuring only one Step Function runs per topic at a time).
*   **DynamoDB:** Supports **Conditional Writes** (`PutItem if_not_exists`). This is the industry standard for serverless distributed locking (used by Terraform S3 backend, etc.).
*   **RDS:** Can do `SELECT FOR UPDATE` or advisory locks, but holding a transaction open inside a stateless Lambda is an anti-pattern and can lead to "zombie locks" if the Lambda times out.

### 4. Operational "Lean-ness"
*   **DynamoDB:** Built-in High Availability (multi-AZ) by default. Zero maintenance windows.
*   **RDS:** High Availability requires Multi-AZ deployment (2x cost). Requires periodic maintenance windows (downtime or failover) for engine version upgrades.

### 5. Automated Lifecycle Management (The "Lean" Cleanup)
*   **Feature:** **Time-To-Live (TTL)**
*   **DynamoDB:** You can set a TTL on individual items (e.g., `expire_at = now() + 7 days`). DynamoDB automatically deletes these items in the background **for free**, without consuming write capacity.
    *   *Use Case:* Clean up old idempotency keys, distributed locks, or processed message logs automatically.
*   **RDS:** Requires a scheduled cron job (`DELETE FROM ... WHERE date < now() - 7 days`).
    *   *Impact:* Deletes cause table bloat, requiring `VACUUM` operations, which consume CPU/IO and can degrade performance of the live pipeline.

### 6. Job Idempotency & Safety
*   **Requirement:** Ensure a batch or job runs exactly once, even if triggered multiple times.
*   **DynamoDB:** Uses **Conditional Writes** (`PutItem ConditionExpression='attribute_not_exists(pk)'`).
    *   *Mechanism:* If a job tries to start but the "lock" key already exists, the write fails instantly (400 Client Error). This provides a hard guarantee against double-processing with zero race conditions.
*   **RDS:** Requires `INSERT ... ON CONFLICT DO NOTHING` or explicit transaction locking. While possible, checking the result of a conditional insert in high-concurrency loops adds latency and connection overhead.

### 7. Long-Term Storage Strategy
*   **DynamoDB (Hot Store)**: Keep only the essential "working set" (e.g., last 7 days of checkpoints/locks). Use TTL to expire old data.
*   **S3/Iceberg (Cold Store)**: If you need to keep state history for compliance (e.g., "Prove we processed batch X last year"):
    1.  Enable **DynamoDB Streams**.
    2.  Capture the `REMOVE` events triggered by TTL.
    3.  Firehose writes these to S3 (Parquet/Iceberg).
    *   *Benefit:* Keeps the hot store lean and fast, while offloading history to the cheapest storage tier. RDS requires complex CDC setup (Debezium/DMS) to achieve the same.

---

## Recommendation

**Stick with DynamoDB.**

Use **RDS** only if:
1.  You need complex ad-hoc queries across checkpoints (e.g., "Show me the average lag time of all topics grouped by hour"). Even then, you can stream DynamoDB to S3/Athena for analytics.
2.  The state data has complex relational integrity constraints that DynamoDB cannot enforce (not applicable for simple checkpoints).
3.   The team effectively bans NoSQL databases (organizational constraint).

**For Lean-Ops, DynamoDB is the technically superior and most cost-effective solution.**
