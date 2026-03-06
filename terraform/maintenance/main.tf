"""
Terraform — Table Maintenance Infrastructure (Parallel Architecture)
=====================================================================

Architecture:
  EventBridge (daily 03:00 UTC)
    → Step Function: LeanOps-TableMaintenance
        State 1: RunInventoryReader (Glue job — reads table_inventory, emits work_items.json)
        State 2: ReadWorkItems (Lambda pass-through to load JSON from S3 → Map input)
        State 3: CompactSubscriptions (Map state, concurrency=10)
                   → For each subscription: RunCompactor (Glue job — compact all 3 tables)
        State 4: Done / Failed

Glue Jobs:
  - lean-ops-inventory-reader  : Reads ODS inventory → S3 JSON
  - lean-ops-table-compactor   : Single-subscription compaction (runs N times in parallel)
  - lean-ops-setup-ops-tables  : One-time DDL setup (not triggered by Step Function)

S3 Lifecycle:
  - data/ prefix → S3-IA after 30 days → Glacier Instant Retrieval after 90 days
  - metadata/ prefix → expire (delete) incomplete multipart uploads after 7 days
"""

# ── Variables ────────────────────────────────────────────────────────────────

variable "iceberg_bucket" {
  description = "S3 bucket where all Iceberg data lives"
  type        = string
}

variable "scripts_bucket" {
  description = "S3 bucket where Glue PySpark scripts are uploaded"
  type        = string
}

variable "ops_bucket" {
  description = "S3 bucket used for operational state (work_items.json, temp files)"
  type        = string
}

variable "raw_database" {
  default = "iceberg_raw_db"
}

variable "ops_database" {
  default = "operational_data_store"
}

variable "map_concurrency" {
  description = "Max parallel Glue jobs launched by the Step Function Map state"
  default     = 10
}

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
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.iceberg_bucket}",  "arn:aws:s3:::${var.iceberg_bucket}/*",
          "arn:aws:s3:::${var.scripts_bucket}", "arn:aws:s3:::${var.scripts_bucket}/*",
          "arn:aws:s3:::${var.ops_bucket}",     "arn:aws:s3:::${var.ops_bucket}/*",
        ]
      },
      { Sid = "GlueCatalog", Effect = "Allow", Action = ["glue:*"], Resource = "*" },
      { Sid = "Logs", Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], Resource = "*" }
    ]
  })
}


# ── Glue Job: ODS Setup (one-time, not in Step Function) ─────────────────────

resource "aws_glue_job" "setup_ops_tables" {
  name              = "lean-ops-setup-ops-tables"
  description       = "One-time: creates operational_data_store Iceberg tables."
  role_arn          = aws_iam_role.glue_maintenance.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue_v2/setup/create_ops_tables.py"
    python_version  = "3"
  }
  default_arguments = {
    "--iceberg_bucket" = var.iceberg_bucket
    "--ops_database"   = var.ops_database
    "--enable-iceberg" = "true"
    "--datalake-formats" = "iceberg"
    "--extra-py-files" = "s3://${var.scripts_bucket}/glue_v2/utils.zip"
    "--TempDir"        = "s3://${var.ops_bucket}/temp/glue/"
  }
}


# ── Glue Job: Inventory Reader ────────────────────────────────────────────────

resource "aws_glue_job" "inventory_reader" {
  name              = "lean-ops-inventory-reader"
  description       = "Reads table_inventory and writes work_items.json to S3 for the Step Function Map state."
  role_arn          = aws_iam_role.glue_maintenance.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue_v2/inventory_reader.py"
    python_version  = "3"
  }
  default_arguments = {
    "--iceberg_bucket"   = var.iceberg_bucket
    "--ops_database"     = var.ops_database
    "--output_bucket"    = var.ops_bucket
    "--enable-iceberg"   = "true"
    "--datalake-formats" = "iceberg"
    "--extra-py-files"   = "s3://${var.scripts_bucket}/glue_v2/utils.zip"
    "--TempDir"          = "s3://${var.ops_bucket}/temp/glue/"
  }
}


# ── Glue Job: Per-Subscription Compactor ─────────────────────────────────────

resource "aws_glue_job" "table_compactor" {
  name              = "lean-ops-table-compactor"
  description       = "Compacts one subscription's RAW/Standardized/Curated tables with tiered strategy and sort."
  role_arn          = aws_iam_role.glue_maintenance.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 4
  timeout           = 120
  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue_v2/table_compactor.py"
    python_version  = "3"
  }
  # Arguments are injected at runtime by the Step Function Map state
  default_arguments = {
    "--iceberg_bucket"   = var.iceberg_bucket
    "--ops_database"     = var.ops_database
    "--enable-iceberg"   = "true"
    "--datalake-formats" = "iceberg"
    "--extra-py-files"   = "s3://${var.scripts_bucket}/glue_v2/utils.zip"
    "--TempDir"          = "s3://${var.ops_bucket}/temp/glue/"
  }
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
      { Effect = "Allow", Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"], Resource = "*" },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject"], Resource = "arn:aws:s3:::${var.ops_bucket}/*" },
      { Effect = "Allow", Action = ["logs:CreateLogGroup", "logs:CreateLogDelivery", "logs:PutLogEvents", "logs:DescribeLogGroups"], Resource = "*" }
    ]
  })
}


# ── Step Function ─────────────────────────────────────────────────────────────

resource "aws_sfn_state_machine" "table_maintenance" {
  name     = "LeanOps-TableMaintenance"
  role_arn = aws_iam_role.sfn_maintenance.arn

  # CloudWatch logging for full Step Function execution visibility
  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_maintenance.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  definition = jsonencode({
    Comment = "Lean-Ops daily table maintenance: inventory → parallel per-subscription compaction."
    StartAt = "RunInventoryReader"
    States = {

      # Stage 1: Run inventory reader — writes work_items.json to ops_bucket
      RunInventoryReader = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.inventory_reader.name
        }
        ResultPath = "$.inventory_result"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "MaintenanceFailed"
          ResultPath  = "$.error"
        }]
        Next = "LoadWorkItems"
      }

      # Stage 2: Load the JSON work_items array from S3 into the execution state
      LoadWorkItems = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:s3:getObject"
        Parameters = {
          Bucket = var.ops_bucket
          Key    = "maintenance/work_items.json"
        }
        ResultSelector = {
          # S3 getObject returns Body as a string; Step Function parses JSON automatically
          "work_items.$" = "States.StringToJson($.Body).work_items"
        }
        ResultPath = "$.loaded"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "MaintenanceFailed"
          ResultPath  = "$.error"
        }]
        Next = "CheckInventory"
      }

      # Stage 3: Skip compaction if no active subscriptions
      CheckInventory = {
        Type    = "Choice"
        Choices = [{
          Variable      = "$.loaded.work_items[0]"
          IsPresent     = true
          Next          = "CompactSubscriptions"
        }]
        Default = "MaintenanceSucceeded"
      }

      # Stage 4: Fan out — one compactor Glue job per subscription, up to N in parallel
      CompactSubscriptions = {
        Type            = "Map"
        MaxConcurrency  = var.map_concurrency
        ItemsPath       = "$.loaded.work_items"
        ItemSelector = {
          # Pass all subscription fields + shared config into each Glue job
          "subscription_name.$"      = "$$.Map.Item.Value.subscription_name"
          "topic_name.$"             = "$$.Map.Item.Value.topic_name"
          "raw_database.$"           = "$$.Map.Item.Value.raw_database"
          "raw_table.$"              = "$$.Map.Item.Value.raw_table"
          "standardized_database.$"  = "$$.Map.Item.Value.standardized_database"
          "standardized_table.$"     = "$$.Map.Item.Value.standardized_table"
          "curated_database.$"       = "$$.Map.Item.Value.curated_database"
          "curated_table.$"          = "$$.Map.Item.Value.curated_table"
        }
        Iterator = {
          StartAt = "RunCompactor"
          States = {
            RunCompactor = {
              Type     = "Task"
              Resource = "arn:aws:states:::glue:startJobRun.sync"
              Parameters = {
                JobName = aws_glue_job.table_compactor.name
                "Arguments.$" = "States.JsonMerge(States.StringToJson('{}'), States.JsonToString($), false)"
                Arguments = {
                  "--subscription_name.$"      = "$.subscription_name"
                  "--topic_name.$"             = "$.topic_name"
                  "--raw_database.$"           = "$.raw_database"
                  "--raw_table.$"              = "$.raw_table"
                  "--standardized_database.$"  = "$.standardized_database"
                  "--standardized_table.$"     = "$.standardized_table"
                  "--curated_database.$"       = "$.curated_database"
                  "--curated_table.$"          = "$.curated_table"
                }
              }
              # Individual compactor failures don't stop other subscriptions
              Catch = [{
                ErrorEquals = ["States.ALL"]
                Next        = "CompactorFailed"
                ResultPath  = "$.error"
              }]
              Next = "CompactorSucceeded"
            }
            CompactorSucceeded = { Type = "Succeed" }
            CompactorFailed = {
              Type  = "Fail"
              Cause = "Compaction failed for one subscription. Check maintenance_log for details."
            }
          }
        }
        ResultPath = "$.compaction_results"
        Next       = "MaintenanceSucceeded"
      }

      MaintenanceSucceeded = { Type = "Succeed" }
      MaintenanceFailed = {
        Type  = "Fail"
        Cause = "Maintenance pipeline failed. Check CloudWatch logs."
      }
    }
  })
}

resource "aws_cloudwatch_log_group" "sfn_maintenance" {
  name              = "/aws/states/LeanOps-TableMaintenance"
  retention_in_days = 30
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
  name = "StartMaintenanceSFN"
  role = aws_iam_role.eventbridge_maintenance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "states:StartExecution", Resource = aws_sfn_state_machine.table_maintenance.arn }]
  })
}


# ── EventBridge: Daily 03:00 UTC ──────────────────────────────────────────────

resource "aws_cloudwatch_event_rule" "daily_maintenance" {
  name                = "lean-ops-daily-table-maintenance"
  description         = "Triggers Iceberg table compaction daily at 03:00 UTC"
  schedule_expression = "cron(0 3 * * ? *)"
}

resource "aws_cloudwatch_event_target" "trigger_maintenance_sfn" {
  rule      = aws_cloudwatch_event_rule.daily_maintenance.name
  target_id = "TriggerMaintenanceSFN"
  arn       = aws_sfn_state_machine.table_maintenance.arn
  role_arn  = aws_iam_role.eventbridge_maintenance.arn
  input     = jsonencode({ trigger = "scheduled" })
}


# ── S3 Lifecycle: Iceberg Data Files ─────────────────────────────────────────
#
# IMPORTANT: Only apply to the `data/` prefix. Iceberg METADATA files
# must remain in S3 Standard — transitioning them causes severe query
# latency degradation.

resource "aws_s3_bucket_lifecycle_configuration" "iceberg_data" {
  bucket = var.iceberg_bucket

  # Transition data files: Standard → IA (30d) → Glacier Instant Retrieval (90d)
  rule {
    id     = "iceberg-data-tiering"
    status = "Enabled"
    filter {
      prefix = "iceberg_raw_db/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "iceberg-std-data-tiering"
    status = "Enabled"
    filter {
      prefix = "iceberg_standardized_db/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }
  }

  rule {
    id     = "iceberg-curated-data-tiering"
    status = "Enabled"
    filter {
      prefix = "iceberg_curated_db/"
    }
    transition {
      days          = 60    # Curated data is queried more often — delay IA transition
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER_IR"
    }
  }

  # Clean up incomplete multipart uploads (Glue/Iceberg can leave these behind on failures)
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter { prefix = "" }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
