# ================================================================
# WHAT HAPPENS AFTER THE LINTER PASSES
# Full pipeline: linter → CD deployer → Glue Enforcer → Rebuilder
# ================================================================
#
# The linter is only STEP 1 of 4. Here is what each component does
# and when it runs.

# ────────────────────────────────────────────────────────────────
# STEP 1 — CI LINTER  (you just ran this)
# Runs: locally / ADO pipeline (no AWS access)
# ────────────────────────────────────────────────────────────────
#
# Output:
#   - Operation type classified
#   - Backward compat result
#   - CD DEPLOYER PREVIEW (plain English forecast)
#   - Exit code 0/1/2
#
# The linter does NOT touch AWS. It does NOT generate SQL.
# It gives you a human-readable forecast so you know what the
# deployer will execute before you open a PR.

# ────────────────────────────────────────────────────────────────
# STEP 2 — CD DEPLOYER  (runs in ADO after PR merges to main)
# Runs: ADO CD pipeline (needs AWS OIDC credentials)
# ────────────────────────────────────────────────────────────────
#
# This is where the ACTUAL Athena DDL SQL is built and executed.
# You cannot run this locally without AWS credentials.
# The actual SQL it generates looks like this:
#
# For SCHEMA_EVOLVE (Scenario 02):
# ┌──────────────────────────────────────────────────────────────┐
# │ ALTER TABLE standardized.std_trades                          │
# │ ADD COLUMNS (                                                │
# │   `settlement_date`  DATE,                                   │
# │   `counterparty_id`  STRING,                                 │
# │   `risk_score`       DOUBLE                                  │
# │ )                                                            │
# └──────────────────────────────────────────────────────────────┘
#
# For NEW_TABLE (Scenario 01):
# ┌──────────────────────────────────────────────────────────────┐
# │ CREATE TABLE standardized.std_options (                      │
# │   `ingest_uuid`         STRING,                              │
# │   `gcp_publish_ts`      TIMESTAMP,                           │
# │   `_std_loaded_at`      TIMESTAMP,                           │
# │   `contract_id`         STRING  COMMENT 'Unique options...'  │
# │   `underlying_ticker`   STRING,                              │
# │   `option_type`         STRING,                              │
# │   `strike_price`        DOUBLE,                              │
# │   `expiry_date`         DATE,                                │
# │   `premium`             DOUBLE,                              │
# │   `trader_ssn`          STRING                               │
# │ )                                                            │
# │ LOCATION 's3://datalake/standardized/std_options/'           │
# │ TBLPROPERTIES (                                              │
# │   'table_type'        = 'ICEBERG',                           │
# │   'format'            = 'parquet',                           │
# │   'write_compression' = 'snappy'                             │
# │ )                                                            │
# └──────────────────────────────────────────────────────────────┘
#
# For NUCLEAR_RESET (Scenario 03):
#   1. Glue API: CreateTable std_trades_archive_20240120093045
#      (same S3 prefix — data preserved and queryable)
#   2. Glue API: DeleteTable std_trades  (catalog entry only — S3 untouched)
#   3. Athena DDL: CREATE TABLE standardized.std_trades
#      (new schema, fresh S3 prefix)
#   4. S3: PUT metadata.json to s3://config-bucket/topics/topic_trades_v3/
#   5. Step Functions: StartExecution HistoricalRebuilderStateMachine
#
# For PURE_REMAP (Scenario 04):
#   Zero DDL executed. Only:
#   1. S3: PUT metadata.json to s3://config-bucket/topics/topic_trades_v2/
#   2. DynamoDB: Update routing record

# ────────────────────────────────────────────────────────────────
# STEP 3 — GLUE ENFORCER JOB  (scheduled PySpark on AWS Glue)
# Runs: AWS Glue — every 5 minutes or on schedule
# ────────────────────────────────────────────────────────────────
#
# The Enforcer runs automatically on schedule. It does NOT know
# what operation type was used — it just reads from AWS.
#
# What it reads at startup (every run):
#   S3         → metadata.json (source_topics, sensitive_columns)
#   Glue Catalog → spark.table("std_trades").schema
#                  This is the KEY design: schema is NOT read from swagger.
#                  It reads the LIVE Glue Catalog column list.
#                  If CD added 3 columns 10 mins ago, the next Enforcer
#                  run automatically picks them up. Zero code change needed.
#   S3         → watermark file (last processed Iceberg snapshot ID)
#
# What it does each run:
#
#  RAW Iceberg table(s)
#       │
#       ▼  (incremental scan from watermark snapshot_id)
#  [raw_df]  columns: ingest_uuid, gcp_publish_ts, payload_string
#       │
#       ▼  If source_topics has multiple entries (e.g. v1 and v2 both active)
#  unionByName(df_v1, df_v2, allowMissingColumns=True)
#       │
#       ▼
#  from_json(payload_string, live_glue_schema)
#       │
#       ├──► payload_parsed IS NULL  →  invalid_df  →  support.error_hub
#       │                                               (ingest_uuid + raw payload preserved)
#       │
#       └──► payload_parsed NOT NULL →  valid_df
#                 │
#                 ▼
#            Flatten struct into top-level columns
#            Apply SHA-256 masking to sensitive_columns
#                 │
#                 ▼
#            MERGE INTO std_trades ON ingest_uuid
#            (idempotent — safe for job retries)
#                 │
#                 ▼
#            Update watermark → last_snapshot_id saved to S3

# ────────────────────────────────────────────────────────────────
# STEP 4 — HISTORICAL REBUILDER  (triggered ONLY by nuclear reset)
# Runs: AWS Step Functions → AWS Glue PySpark job
# ────────────────────────────────────────────────────────────────
#
# After a nuclear reset, the new std_trades table is EMPTY.
# Historical records are safe in std_trades_archive_<ts> but they
# were written against the OLD schema. They need to be re-parsed
# against the NEW Glue Catalog schema.
#
# Trigger: CD deployer calls Step Functions StartExecution immediately
#          after the CREATE TABLE DDL succeeds.
#
# Input to Step Functions:
#   {
#     "target_std_table": "std_trades",
#     "source_topics":    ["topic_trades_v1", "topic_trades_v2", "topic_trades_v3"],
#     "archive_table":    "std_trades_archive_20240120093045"
#   }
#
# What the Rebuilder PySpark job does:
#
#  Read ALL RAW table history (full scan, no watermark filter)
#  for EVERY topic in source_topics
#       │
#       ▼
#  unionByName all source DataFrames
#       │
#       ▼
#  Read NEW schema from Glue Catalog
#  spark.table("glue_catalog.standardized.std_trades").schema
#       │
#       ▼
#  from_json(payload_string, new_schema)
#  Apply masking to sensitive_columns
#       │
#       ├──► invalid → support.error_hub (tagged job_type=historical_rebuild)
#       │
#       └──► valid → repartition for large write performance
#                 → writeTo std_trades .append()
#                   (plain append, NOT MERGE — table is empty so no dedup needed)
#                 → set watermark to current RAW snapshot
#                 → notify Step Functions: SUCCEEDED
#
# Step Functions checks the result:
#   SUCCEEDED → enable Enforcer scheduled trigger → normal processing resumes
#   FAILED    → alert ops team, keep Enforcer paused, await manual intervention
#
# Typical Rebuilder duration:
#   Small tables  (< 10M rows)  : 5-15 minutes
#   Medium tables (10M-100M)    : 30-90 minutes
#   Large tables  (> 100M rows) : hours — plan the maintenance window accordingly
#
# The Enforcer is kept PAUSED (flow_control_flag=PAUSED in DynamoDB)
# for the entire duration of the Rebuilder run. This prevents live records
# from being written to std_trades while the historical rebuild is in progress.
# Once the Rebuilder signals SUCCEEDED, the flag is set to ENABLED
# and the Enforcer picks up from the watermark the Rebuilder set.

# ────────────────────────────────────────────────────────────────
# SUMMARY: WHICH TOOL RUNS WHEN
# ────────────────────────────────────────────────────────────────
#
#  Developer action      → Tool that runs            → Where
#  ───────────────────────────────────────────────────────────────
#  Push to feature branch
#  + trigger manually    → ci_linter.py (validate)   → ADO pipeline (no AWS)
#
#  Open PR               → ci_linter.py (gate)       → ADO pipeline (no AWS)
#
#  PR merges to main     → cd_deployer.py             → ADO pipeline (AWS via OIDC)
#                          executes actual Athena DDL
#
#  Scheduled trigger     → Glue Enforcer Job          → AWS Glue (PySpark)
#  (every 5 min)           reads live Glue schema
#                          parses RAW → STD
#
#  After nuclear reset   → Historical Rebuilder        → AWS Step Functions
#  (automatic)             re-parses ALL RAW history    → AWS Glue (PySpark)
#                          against new schema
