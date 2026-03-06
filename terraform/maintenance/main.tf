# ==============================================================================
# Terraform — Table Maintenance: Layer-Independent Step Functions
# ==============================================================================
#
# Architecture:
#  Three independent Step Functions, each triggered by its own EventBridge rule:
#
#  LeanOps-CompactRAW         02:00 UTC daily
#    ReadInventory(RAW) → Map[Compact+Expire] → Done
#
#  LeanOps-CompactStandardized  02:30 UTC daily
#    ReadInventory(STD) → Map[Compact+Expire] → Done
#
#  LeanOps-CompactCurated      03:00 UTC daily
#    ReadInventory(CUR) → Map[Compact, skip_expiry=true]
#                       → Wait 5 min (Snowflake REST catalog refresh buffer)
#                       → Map[ExpireOnly, skip_expiry=false]
#                       → Done
#
# Glue Jobs:
#  lean-ops-inventory-reader  : layer-aware, writes work_items_<layer>.json
#  lean-ops-table-compactor   : single-table, accepts --layer --skip_expiry
#  lean-ops-setup-ops-tables  : one-time DDL (not in any Step Function)

# ── Variables ────────────────────────────────────────────────────────────────

variable "iceberg_bucket" { type = string }
variable "scripts_bucket" { type = string }
variable "ops_bucket"     { type = string }
variable "raw_database"   { default = "iceberg_raw_db" }
variable "ops_database"   { default = "operational_data_store" }
variable "map_concurrency"{ default = 10 }

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# ── IAM — Glue Role ───────────────────────────────────────────────────────────

resource "aws_iam_role" "glue_maintenance" {
  name = "lean-ops-glue-maintenance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_maintenance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
resource "aws_iam_role_policy" "glue_permissions" {
  name = "GlueMaintenancePermissions"
  role = aws_iam_role.glue_maintenance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "S3", Effect = "Allow", Action = ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
        Resource = [
          "arn:aws:s3:::${var.iceberg_bucket}", "arn:aws:s3:::${var.iceberg_bucket}/*",
          "arn:aws:s3:::${var.scripts_bucket}", "arn:aws:s3:::${var.scripts_bucket}/*",
          "arn:aws:s3:::${var.ops_bucket}",     "arn:aws:s3:::${var.ops_bucket}/*",
        ]},
      { Sid = "Glue",  Effect = "Allow", Action = ["glue:*"], Resource = "*" },
      { Sid = "Logs",  Effect = "Allow", Action = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"], Resource = "*" }
    ]
  })
}

# ── IAM — Step Function Role ──────────────────────────────────────────────────

resource "aws_iam_role" "sfn_maintenance" {
  name = "lean-ops-sfn-maintenance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "states.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy" "sfn_permissions" {
  name = "SFNMaintenancePermissions"
  role = aws_iam_role.sfn_maintenance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["glue:StartJobRun","glue:GetJobRun","glue:GetJobRuns"], Resource = "*" },
      { Effect = "Allow", Action = ["s3:GetObject","s3:PutObject"], Resource = "arn:aws:s3:::${var.ops_bucket}/*" },
      { Effect = "Allow", Action = ["logs:CreateLogGroup","logs:CreateLogDelivery","logs:PutLogEvents","logs:DescribeLogGroups"], Resource = "*" }
    ]
  })
}

# ── IAM — EventBridge Role ────────────────────────────────────────────────────

resource "aws_iam_role" "eventbridge_maintenance" {
  name = "lean-ops-eventbridge-maintenance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "scheduler.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy" "eventbridge_sfn" {
  name = "StartMaintenanceSFNs"
  role = aws_iam_role.eventbridge_maintenance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = [
      aws_sfn_state_machine.compact_raw.arn,
      aws_sfn_state_machine.compact_standardized.arn,
      aws_sfn_state_machine.compact_curated.arn,
    ]}]
  })
}

# ── Glue Jobs ─────────────────────────────────────────────────────────────────

locals {
  glue_common_args = {
    "--iceberg_bucket"    = var.iceberg_bucket
    "--ops_database"      = var.ops_database
    "--enable-iceberg"    = "true"
    "--datalake-formats"  = "iceberg"
    "--extra-py-files"    = "s3://${var.scripts_bucket}/glue_v2/utils.zip"
    "--TempDir"           = "s3://${var.ops_bucket}/temp/glue/"
  }
}

resource "aws_glue_job" "setup_ops_tables" {
  name = "lean-ops-setup-ops-tables"
  role_arn = aws_iam_role.glue_maintenance.arn
  glue_version = "4.0"; worker_type = "G.1X"; number_of_workers = 2
  command { name = "glueetl"; script_location = "s3://${var.scripts_bucket}/glue_v2/setup/create_ops_tables.py"; python_version = "3" }
  default_arguments = merge(local.glue_common_args, {})
}

resource "aws_glue_job" "inventory_reader" {
  name        = "lean-ops-inventory-reader"
  description = "Layer-aware: reads table_inventory and emits work_items_<layer>.json"
  role_arn    = aws_iam_role.glue_maintenance.arn
  glue_version = "4.0"; worker_type = "G.1X"; number_of_workers = 2
  command { name = "glueetl"; script_location = "s3://${var.scripts_bucket}/glue_v2/inventory_reader.py"; python_version = "3" }
  default_arguments = merge(local.glue_common_args, {
    "--output_bucket"  = var.ops_bucket
    "--raw_database"   = var.raw_database
    "--layer"          = "RAW"   # overridden at runtime by each Step Function
  })
}

resource "aws_glue_job" "table_compactor" {
  name        = "lean-ops-table-compactor"
  description = "Single-table compactor: --layer, --skip_expiry controls Snowflake-safe expiry"
  role_arn    = aws_iam_role.glue_maintenance.arn
  glue_version = "4.0"; worker_type = "G.1X"; number_of_workers = 4; timeout = 120
  command { name = "glueetl"; script_location = "s3://${var.scripts_bucket}/glue_v2/table_compactor.py"; python_version = "3" }
  default_arguments = merge(local.glue_common_args, {
    "--skip_expiry" = "false"
  })
}

# ── CloudWatch Log Groups ─────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "sfn_raw" {
  name = "/aws/states/LeanOps-CompactRAW"; retention_in_days = 30
}
resource "aws_cloudwatch_log_group" "sfn_std" {
  name = "/aws/states/LeanOps-CompactStandardized"; retention_in_days = 30
}
resource "aws_cloudwatch_log_group" "sfn_cur" {
  name = "/aws/states/LeanOps-CompactCurated"; retention_in_days = 30
}

# ── Helper: Reusable Step Function state blocks ───────────────────────────────
# Terraform doesn't support partial template functions, so each SFN is defined
# explicitly. All three share the same ReadInventory + LoadWorkItems + CheckInventory
# pattern, but diverge at the Map state to pass the correct --layer and --skip_expiry.

locals {
  # Shared Map item_selector args injected into each Glue job
  compactor_base_args = {
    "--topic_name.$"      = "$.topic_name"
    "--ingestion_mode.$"  = "$.ingestion_mode"
    "--database.$"        = "$.database"
    "--table.$"           = "$.table"
    "--layer.$"           = "$.layer"
    "--ops_database"      = var.ops_database
    "--iceberg_bucket"    = var.iceberg_bucket
    "--enable-iceberg"    = "true"
    "--datalake-formats"  = "iceberg"
    "--TempDir"           = "s3://${var.ops_bucket}/temp/glue/"
  }
}

# ── Step Function: RAW (02:00 UTC) ────────────────────────────────────────────

resource "aws_sfn_state_machine" "compact_raw" {
  name     = "LeanOps-CompactRAW"
  role_arn = aws_iam_role.sfn_maintenance.arn
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_raw.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
  definition = jsonencode({
    Comment = "RAW layer compaction — runs daily 02:00 UTC"
    StartAt = "ReadInventory"
    States = {
      ReadInventory = {
        Type = "Task"; Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.inventory_reader.name
          Arguments = { "--layer" = "RAW", "--raw_database" = var.raw_database } }
        ResultPath = "$.inv"; Next = "LoadWorkItems"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Failed", ResultPath = "$.error" }]
      }
      LoadWorkItems = {
        Type = "Task"; Resource = "arn:aws:states:::aws-sdk:s3:getObject"
        Parameters = { Bucket = var.ops_bucket, Key = "maintenance/work_items_raw.json" }
        ResultSelector = { "work_items.$" = "States.StringToJson($.Body).work_items" }
        ResultPath = "$.loaded"; Next = "CheckItems"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Failed", ResultPath = "$.error" }]
      }
      CheckItems = {
        Type = "Choice"
        Choices = [{ Variable = "$.loaded.work_items[0]", IsPresent = true, Next = "CompactMap" }]
        Default = "Succeeded"
      }
      CompactMap = {
        Type = "Map"; MaxConcurrency = var.map_concurrency
        ItemsPath = "$.loaded.work_items"
        Iterator = {
          StartAt = "Compact"
          States = {
            Compact = {
              Type = "Task"; Resource = "arn:aws:states:::glue:startJobRun.sync"
              Parameters = {
                JobName = aws_glue_job.table_compactor.name
                Arguments = merge(local.compactor_base_args, { "--skip_expiry" = "false" })
              }
              Catch = [{ ErrorEquals = ["States.ALL"], Next = "ItemFailed", ResultPath = "$.error" }]
              Next = "ItemDone"
            }
            ItemDone   = { Type = "Succeed" }
            ItemFailed = { Type = "Fail", Cause = "RAW compaction failed for one table — check maintenance_log" }
          }
        }
        Next = "Succeeded"
      }
      Succeeded = { Type = "Succeed" }
      Failed    = { Type = "Fail", Cause = "RAW maintenance pipeline error — check CloudWatch logs" }
    }
  })
}

# ── Step Function: STANDARDIZED (02:30 UTC) ───────────────────────────────────

resource "aws_sfn_state_machine" "compact_standardized" {
  name     = "LeanOps-CompactStandardized"
  role_arn = aws_iam_role.sfn_maintenance.arn
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_std.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
  definition = jsonencode({
    Comment = "Standardized layer compaction — runs daily 02:30 UTC"
    StartAt = "ReadInventory"
    States = {
      ReadInventory = {
        Type = "Task"; Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.inventory_reader.name
          Arguments = { "--layer" = "STANDARDIZED" } }
        ResultPath = "$.inv"; Next = "LoadWorkItems"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Failed", ResultPath = "$.error" }]
      }
      LoadWorkItems = {
        Type = "Task"; Resource = "arn:aws:states:::aws-sdk:s3:getObject"
        Parameters = { Bucket = var.ops_bucket, Key = "maintenance/work_items_standardized.json" }
        ResultSelector = { "work_items.$" = "States.StringToJson($.Body).work_items" }
        ResultPath = "$.loaded"; Next = "CheckItems"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Failed", ResultPath = "$.error" }]
      }
      CheckItems = {
        Type = "Choice"
        Choices = [{ Variable = "$.loaded.work_items[0]", IsPresent = true, Next = "CompactMap" }]
        Default = "Succeeded"
      }
      CompactMap = {
        Type = "Map"; MaxConcurrency = var.map_concurrency
        ItemsPath = "$.loaded.work_items"
        Iterator = {
          StartAt = "Compact"
          States = {
            Compact = {
              Type = "Task"; Resource = "arn:aws:states:::glue:startJobRun.sync"
              Parameters = {
                JobName = aws_glue_job.table_compactor.name
                Arguments = merge(local.compactor_base_args, { "--skip_expiry" = "false" })
              }
              Catch = [{ ErrorEquals = ["States.ALL"], Next = "ItemFailed", ResultPath = "$.error" }]
              Next = "ItemDone"
            }
            ItemDone   = { Type = "Succeed" }
            ItemFailed = { Type = "Fail", Cause = "Standardized compaction failed for one table — check maintenance_log" }
          }
        }
        Next = "Succeeded"
      }
      Succeeded = { Type = "Succeed" }
      Failed    = { Type = "Fail", Cause = "Standardized maintenance error — check CloudWatch logs" }
    }
  })
}

# ── Step Function: CURATED (03:00 UTC) — Snowflake-safe expiry ───────────────
#
# Curated tables are exposed to Snowflake via Iceberg REST catalog (120s refresh).
# Pattern:
#   1. CompactMap (skip_expiry=true)  — rewrite data files; old files still present
#   2. Wait 5 minutes                 — enough for Snowflake REST refresh (2× 120s + buffer)
#   3. ExpireMap (skip_expiry=false)  — now safe to delete old snapshot files

resource "aws_sfn_state_machine" "compact_curated" {
  name     = "LeanOps-CompactCurated"
  role_arn = aws_iam_role.sfn_maintenance.arn
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_cur.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }
  definition = jsonencode({
    Comment = "Curated layer compaction with Snowflake-safe expiry buffer — runs daily 03:00 UTC"
    StartAt = "ReadInventory"
    States = {
      ReadInventory = {
        Type = "Task"; Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = { JobName = aws_glue_job.inventory_reader.name
          Arguments = { "--layer" = "CURATED" } }
        ResultPath = "$.inv"; Next = "LoadWorkItems"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Failed", ResultPath = "$.error" }]
      }
      LoadWorkItems = {
        Type = "Task"; Resource = "arn:aws:states:::aws-sdk:s3:getObject"
        Parameters = { Bucket = var.ops_bucket, Key = "maintenance/work_items_curated.json" }
        ResultSelector = { "work_items.$" = "States.StringToJson($.Body).work_items" }
        ResultPath = "$.loaded"; Next = "CheckItems"
        Catch = [{ ErrorEquals = ["States.ALL"], Next = "Failed", ResultPath = "$.error" }]
      }
      CheckItems = {
        Type = "Choice"
        Choices = [{ Variable = "$.loaded.work_items[0]", IsPresent = true, Next = "CompactMap" }]
        Default = "Succeeded"
      }

      # Phase 1: Compact — skip_expiry=true so old snapshot files stay on S3
      CompactMap = {
        Type = "Map"; MaxConcurrency = var.map_concurrency
        ItemsPath = "$.loaded.work_items"
        ResultPath = "$.compaction_results"
        Iterator = {
          StartAt = "Compact"
          States = {
            Compact = {
              Type = "Task"; Resource = "arn:aws:states:::glue:startJobRun.sync"
              Parameters = {
                JobName = aws_glue_job.table_compactor.name
                Arguments = merge(local.compactor_base_args, { "--skip_expiry" = "true" })
              }
              Catch = [{ ErrorEquals = ["States.ALL"], Next = "ItemFailed", ResultPath = "$.error" }]
              Next = "ItemDone"
            }
            ItemDone   = { Type = "Succeed" }
            ItemFailed = { Type = "Fail", Cause = "Curated compaction failed for one table" }
          }
        }
        Next = "SnowflakeBuffer"
      }

      # Phase 2: Wait — 5 minutes = 300 seconds
      # Allows Snowflake REST catalog (120s refresh) to pick up the new
      # compacted snapshot before we delete the files it may still reference.
      SnowflakeBuffer = {
        Type    = "Wait"
        Seconds = 300
        Next    = "ExpireMap"
      }

      # Phase 3: Expire — now safe to delete old snapshot files
      ExpireMap = {
        Type = "Map"; MaxConcurrency = var.map_concurrency
        ItemsPath = "$.loaded.work_items"
        Iterator = {
          StartAt = "Expire"
          States = {
            Expire = {
              Type = "Task"; Resource = "arn:aws:states:::glue:startJobRun.sync"
              Parameters = {
                JobName = aws_glue_job.table_compactor.name
                Arguments = merge(local.compactor_base_args, {
                  "--skip_expiry" = "false"
                  # Pass empty partition list by setting a past-today date
                  # so the compactor only runs expire_snapshots, not rewrite
                  "--force_expire_only" = "true"
                })
              }
              Catch = [{ ErrorEquals = ["States.ALL"], Next = "ExpireFailed", ResultPath = "$.error" }]
              Next = "ExpireDone"
            }
            ExpireDone   = { Type = "Succeed" }
            ExpireFailed = { Type = "Fail", Cause = "Snapshot expiry failed — files still present, no data loss" }
          }
        }
        Next = "Succeeded"
      }

      Succeeded = { Type = "Succeed" }
      Failed    = { Type = "Fail", Cause = "Curated maintenance error — check CloudWatch logs" }
    }
  })
}

# ── EventBridge: Three independent schedules ──────────────────────────────────

resource "aws_cloudwatch_event_rule" "compact_raw" {
  name = "lean-ops-compact-raw"
  description = "Daily RAW compaction 02:00 UTC"
  schedule_expression = "cron(0 2 * * ? *)"
}
resource "aws_cloudwatch_event_target" "compact_raw" {
  rule     = aws_cloudwatch_event_rule.compact_raw.name
  target_id = "CompactRAWSFN"
  arn      = aws_sfn_state_machine.compact_raw.arn
  role_arn = aws_iam_role.eventbridge_maintenance.arn
}

resource "aws_cloudwatch_event_rule" "compact_standardized" {
  name = "lean-ops-compact-standardized"
  description = "Daily Standardized compaction 02:30 UTC"
  schedule_expression = "cron(30 2 * * ? *)"
}
resource "aws_cloudwatch_event_target" "compact_standardized" {
  rule     = aws_cloudwatch_event_rule.compact_standardized.name
  target_id = "CompactStdSFN"
  arn      = aws_sfn_state_machine.compact_standardized.arn
  role_arn = aws_iam_role.eventbridge_maintenance.arn
}

resource "aws_cloudwatch_event_rule" "compact_curated" {
  name = "lean-ops-compact-curated"
  description = "Daily Curated compaction 03:00 UTC (Snowflake-safe)"
  schedule_expression = "cron(0 3 * * ? *)"
}
resource "aws_cloudwatch_event_target" "compact_curated" {
  rule     = aws_cloudwatch_event_rule.compact_curated.name
  target_id = "CompactCuratedSFN"
  arn      = aws_sfn_state_machine.compact_curated.arn
  role_arn = aws_iam_role.eventbridge_maintenance.arn
}

# ── S3 Lifecycle ──────────────────────────────────────────────────────────────

resource "aws_s3_bucket_lifecycle_configuration" "iceberg_data" {
  bucket = var.iceberg_bucket
  rule {
    id = "raw-tiering"; status = "Enabled"
    filter { prefix = "iceberg_raw_db/" }
    transition { days = 30;  storage_class = "STANDARD_IA" }
    transition { days = 90;  storage_class = "GLACIER_IR"  }
  }
  rule {
    id = "std-tiering"; status = "Enabled"
    filter { prefix = "iceberg_standardized_db/" }
    transition { days = 30;  storage_class = "STANDARD_IA" }
    transition { days = 90;  storage_class = "GLACIER_IR"  }
  }
  rule {
    id = "curated-tiering"; status = "Enabled"
    filter { prefix = "iceberg_curated_db/" }
    transition { days = 60;  storage_class = "STANDARD_IA" }
    transition { days = 180; storage_class = "GLACIER_IR"  }
  }
  rule {
    id = "abort-multipart"; status = "Enabled"
    filter { prefix = "" }
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}
