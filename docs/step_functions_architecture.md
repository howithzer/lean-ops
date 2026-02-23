# Lean-Ops Step Functions Architecture

## Overview
This document outlines the proposed architecture for dynamically orchestrating Glue jobs across multiple tables using AWS Step Functions.

## Architecture design

We need an orchestration layer that triggers the Data Pipeline (Standardized + Curated mapping) across an arbitrary, growing number of ingested topic tables. 

### DynamoDB Registry
To track all active tables that need processing, we will have a DynamoDB table: `PipelineRegistry`.

**Partition Key:** `topic_name` (e.g. `topic_trades_v2`, `topic_options_v1`)
**Attributes:**
- `is_active`: boolean (controls if the pipeline should process this topic)
- `flow_control_flag`: ENABLED | PAUSED (Paused during historical rebuilding)
- `last_updated_at`: timestamp

### Step Function Orchestrator

Rather than hardcoding table names or creating a schedule per table, we will have a single **Master Orchestrator Step Function** that runs on a schedule (e.g. every 15 minutes).

1. **Fetch Active Topics:**
   - The first step is a Lambda function (`FetchActiveTopicsLambda`) that scans the `PipelineRegistry` DynamoDB table and returns a list of all active topics where `flow_control_flag == ENABLED`.
   
2. **Map State (Dynamic Parallel Execution):**
   - The Step Function utilizes a `Map` state. It takes the array of `topic_name`s returned by the first Lambda.
   - For *each* topic in the array, it concurrently spins up an isolated execution branch.
   - *Crucially:* We configure `ToleratedFailurePercentage: 100` on the Map state. If the `topic_options_v1` branch fails (e.g., bad data, CDE spike), it does **not** stop the `topic_trades_v2` branch from running.

3. **Per-Topic Execution Branch (Inside the Map State):**
   - **Start Standardized Job:** Triggers the Glue Job (`standardized_processor.py`), passing `--topic_name=topic_X`.
   - **Wait for Standardized:** Polls until the Standardized job succeeds.
   - **Start Curated Job:** Triggers the Glue Job (`curated_processor.py`), passing `--topic_name=topic_X`.
   - **Wait for Curated:** Polls until the Curated job succeeds.
   - **Catch Errors:** If any job fails, the branch catches the error, logs a metric to CloudWatch (or SNS), and terminates the branch as `FAILED`, isolating the failure to just that topic.

### Integration with Schema Management

Yes, merging the `schema_test_kit` and `cd_deployer.py` concepts into the `lean-ops` repository is the correct path. 

**How they interact:**
1. A developer opens a PR in `lean-ops` to modify a schema.
2. The `ci_linter.py` runs in the PR gate to validate backward compatibility.
3. The PR merges.
4. The `cd_deployer.py` runs. 
   - If it's a new table (`NEW_TABLE`), the deployer simply adds `topic_new_table` to the `PipelineRegistry` DynamoDB table as `ENABLED`. The Orchestrator automatically picks it up on its next 15-minute run.
   - If it's an evolution (`SCHEMA_EVOLVE`), no pipeline orchestration changes are needed.
   - If it's a breaking change (`NUCLEAR_RESET`):
     1. The deployer updates the `PipelineRegistry` for that topic to `flow_control_flag = PAUSED`.
     2. The deployer triggers the **Historical Rebuilder Step Function** specifically for that topic.
     3. The Rebuilder runs. When finished, its final step updates the `PipelineRegistry` back to `ENABLED`.
     4. The normal Orchestrator resumes processing that topic on its next cycle.

## Impact on Glue Jobs

**Do we need to parameterize the Glue Jobs?**
Yes. You already have this exactly right in your existing scripts:
```python
args = getResolvedOptions(sys.argv, ['topic_name', ...])
TOPIC_NAME = args['topic_name']
```
Because your Glue jobs are dynamically looking up `TABLE_NAME = f"iceberg_..._{TOPIC_NAME}"` and pulling the checkpoint state using the `topic_name`, the exact same Spark code can process trade events and option events by just passing a different argument.

**Do we need new Historical Rebuilder Spark Jobs?**
Yes. You will need one new Glue Job script: `historical_rebuilder.py`.
This script will be heavily based on `standardized_processor.py`, but instead of using `read_incremental_from_raw()` with `start_snapshot/end_snapshot`, it will do a full table scan `spark.read.load()` on the RAW layers, parse against the new catalog schema, and do an `.append()` write to the newly recreated Standardized table.
