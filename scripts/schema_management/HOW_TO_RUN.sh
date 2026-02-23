# Schema Management — Local Test Kit
# =====================================
# Run the CI linter offline with sample files covering all four operation
# types and all deliberate failure cases.
#
# No AWS credentials needed. No Docker. No Glue. Pure Python.

# ─────────────────────────────────────────────────────────
# PREREQUISITES — one-time setup
# ─────────────────────────────────────────────────────────

pip install pyyaml

# ─────────────────────────────────────────────────────────
# FILE STRUCTURE
# ─────────────────────────────────────────────────────────
#
# schema_test_kit/
# ├── ci_linter.py                    ← linter script
# ├── shared/
# │   └── swagger_v1.yaml             ← baseline (simulates main branch)
# └── scenarios/
#     ├── 01_new_table/
#     │   ├── swagger_v2.yaml         ← new topic, no v1 exists
#     │   └── metadata.json
#     ├── 02_schema_evolve/
#     │   ├── swagger_v2.yaml         ← 3 new optional fields added
#     │   └── metadata.json
#     ├── 03_nuclear_reset/
#     │   ├── swagger_v2.yaml         ← breaking changes, nuclear=true
#     │   └── metadata.json           ← has justification + impact fields
#     ├── 04_pure_remap/
#     │   ├── swagger_v2.yaml         ← identical to v1, just new topic name
#     │   └── metadata.json
#     └── 05_fail_cases/
#         ├── swagger_removed_field.yaml        ← ssn removed
#         ├── swagger_type_changed.yaml         ← amount: number → string
#         ├── swagger_new_required_field.yaml   ← instrument_type added as required
#         ├── metadata_missing_field.json       ← target_std_table absent
#         ├── metadata_nuclear_no_justification.json
#         └── metadata_bad_sensitive_col.json   ← passport_number not in swagger

# ─────────────────────────────────────────────────────────
# WHAT THE LINTER OUTPUTS
# ─────────────────────────────────────────────────────────
#
# Every run prints:
#   1. Operation type classified  (NEW_TABLE / SCHEMA_EVOLVE / NUCLEAR_RESET / PURE_REMAP)
#   2. Field-by-field diff results (new optional = INFO, breaking = ERROR)
#   3. CD DEPLOYER PREVIEW — the human-readable forecast of what will execute on merge
#   4. RESULT line with exit code
#
# Exit codes (ADO reads these):
#   0 = PASS
#   1 = FAIL — breaking change without nuclear override
#   2 = CONFIG ERROR — bad files, missing fields (pipeline setup problem)
#
# NOTE: The linter does NOT show the actual Athena SQL.
# The actual SQL (CREATE TABLE / ALTER TABLE DDL) is built and submitted
# by cd_deployer.py after the PR merges. The linter gives you a
# plain-English preview so you know what will happen before you open the PR.

# ─────────────────────────────────────────────────────────
# SCENARIO 01 — NEW TABLE
# ─────────────────────────────────────────────────────────
# Situation: brand new topic, no schema exists on main yet.
# Flag:      --first-deploy skips the diff (nothing to compare against).
# Expected:  PASS | Operation: NEW_TABLE
# CD will:   CREATE TABLE standardized.std_options (7 columns)

python ci_linter.py \
  --v2       scenarios/01_new_table/swagger_v2.yaml \
  --metadata scenarios/01_new_table/metadata.json \
  --first-deploy

# ─────────────────────────────────────────────────────────
# SCENARIO 02 — SCHEMA EVOLVE (additive, safe)
# ─────────────────────────────────────────────────────────
# Situation: producer adds 3 new optional fields to an existing topic.
# Expected:  PASS | Operation: SCHEMA_EVOLVE
# CD will:   ALTER TABLE std_trades ADD COLUMNS (counterparty_id, risk_score, settlement_date)

python ci_linter.py \
  --v1       shared/swagger_v1.yaml \
  --v2       scenarios/02_schema_evolve/swagger_v2.yaml \
  --metadata scenarios/02_schema_evolve/metadata.json

# ─────────────────────────────────────────────────────────
# SCENARIO 03 — NUCLEAR RESET (breaking change, approved)
# ─────────────────────────────────────────────────────────
# Situation: producer removes fields and adds a required field.
#            nuclear_reset=true with justification provided.
# Expected:  PASS (override) | Operation: NUCLEAR_RESET
# CD will:   RENAME std_trades → std_trades_archive_<ts>
#            CREATE TABLE std_trades (new schema, fresh S3 prefix)
#            TRIGGER Step Functions Rebuilder job

python ci_linter.py \
  --v1       shared/swagger_v1.yaml \
  --v2       scenarios/03_nuclear_reset/swagger_v2.yaml \
  --metadata scenarios/03_nuclear_reset/metadata.json

# ─────────────────────────────────────────────────────────
# SCENARIO 04 — PURE REMAP (new topic name, schema unchanged)
# ─────────────────────────────────────────────────────────
# Situation: GCP PubSub forced topic_trades_v2 but payload is identical.
# Expected:  PASS | Operation: PURE_REMAP
# CD will:   Zero DDL. Only metadata.json uploaded to S3.
#            DynamoDB routing record updated.

python ci_linter.py \
  --v1       shared/swagger_v1.yaml \
  --v2       scenarios/04_pure_remap/swagger_v2.yaml \
  --metadata scenarios/04_pure_remap/metadata.json

# ─────────────────────────────────────────────────────────
# SCENARIO 05 — DELIBERATE FAILURES
# ─────────────────────────────────────────────────────────

# 05a: Field removed — exit 1 (but caught earlier as exit 2 because
#      ssn is in sensitive_columns and was removed from swagger too)
python ci_linter.py \
  --v1       shared/swagger_v1.yaml \
  --v2       scenarios/05_fail_cases/swagger_removed_field.yaml \
  --metadata scenarios/02_schema_evolve/metadata.json

# 05b: Type changed — exit 1
python ci_linter.py \
  --v1       shared/swagger_v1.yaml \
  --v2       scenarios/05_fail_cases/swagger_type_changed.yaml \
  --metadata scenarios/02_schema_evolve/metadata.json

# 05c: New required field — exit 1
python ci_linter.py \
  --v1       shared/swagger_v1.yaml \
  --v2       scenarios/05_fail_cases/swagger_new_required_field.yaml \
  --metadata scenarios/02_schema_evolve/metadata.json

# 05d: metadata.json missing target_std_table — exit 2
python ci_linter.py \
  --v2       scenarios/02_schema_evolve/swagger_v2.yaml \
  --metadata scenarios/05_fail_cases/metadata_missing_field.json

# 05e: nuclear_reset=true but no justification — exit 2
python ci_linter.py \
  --v1       shared/swagger_v1.yaml \
  --v2       scenarios/03_nuclear_reset/swagger_v2.yaml \
  --metadata scenarios/05_fail_cases/metadata_nuclear_no_justification.json

# 05f: sensitive_columns references a field not in swagger — exit 2
python ci_linter.py \
  --v2       scenarios/02_schema_evolve/swagger_v2.yaml \
  --metadata scenarios/05_fail_cases/metadata_bad_sensitive_col.json
