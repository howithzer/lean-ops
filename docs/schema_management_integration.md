# Schema Management & Data Pipeline Integration

This document outlines the end-to-end lifecycle of schema deployments and how they integrate with the Step Functions data orchestrator gracefully.

## 1. Schema Lifecycle 

The pipeline guarantees safe schema evolution without data loss or pipeline interruption.

### Step 1: CI Linter (PR Gate)
When a developer modifies a schema file (e.g., `events.json`) and opens a Pull Request:
1. `ci_linter.py` runs and compares the modified schema against the main branch.
2. It classifying the change as one of four scenarios: `NEW_TABLE`, `SCHEMA_EVOLVE`, `NUCLEAR_RESET`, or `PURE_REMAP`.
3. It blocks the PR if breaking changes are detected but not intentionally declared.

### Step 2: CD Deployer (Merge to Main)
Once the PR is merged, `cd_deployer.py` runs with AWS credentials to deploy the schema.
- **NEW_TABLE:** Creates the Iceberg table using Athena DDL. Adds the topic to the `PipelineRegistry` DynamoDB table (`flow_control_flag = ENABLED`).
- **SCHEMA_EVOLVE:** New columns are inherently supported by the pipeline. No structural changes needed.
- **NUCLEAR_RESET (Breaking Change):** 
  1. Renames the old Iceberg table to an archive table.
  2. Creates a new Iceberg table against the new schema.
  3. Pauses the pipeline for this topic (`flow_control_flag = PAUSED`).
  4. Triggers the **Historical Rebuilder** Step Function to rebuild the entire table history from the RAW layer against the new schema.

### Step 3: Master Pipeline Orchestrator
Running on a 15-minute schedule, the Step Function Master Orchestrator checks the `PipelineRegistry`.
- It fetches all active topics where `flow_control_flag == ENABLED`.
- Uses a Distributed Map State to run Glue Jobs (`standardized_processor.py` → `curated_processor.py`) for all topics concurrently.
- If a topic is `PAUSED` (due to a running Historical Rebuilder), the Master Orchestrator simply skips it, preventing live records from being written during the rebuild.

### Step 4: Historical Rebuilder
Triggered by the CD Deployer during a `NUCLEAR_RESET`:
1. Reads all historical data from the `RAW` Iceberg staging table (ignoring normal incremental checkpoints).
2. Reparses and flattens all historical JSON payloads against the *new* Glue catalog schema.
3. Appends all records into the newly created Standardized table.
4. Updates the standard checkpoints bookmark so the Master Orchestrator knows where to resume.
5. Re-enables the topic in the `PipelineRegistry` (`flow_control_flag = ENABLED`).
