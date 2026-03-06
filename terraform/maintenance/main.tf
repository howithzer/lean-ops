"""
Terraform — Table Maintenance Infrastructure
=============================================

Resources:
  1. Glue Jobs
     - lean-ops-setup-ops-tables  : One-time DDL job to create ODS tables
     - lean-ops-table-compactor   : Daily compaction job

  2. Step Function
     - LeanOps-TableMaintenance   : Two sequential Glue job invocations with
                                    error handling states.

  3. EventBridge Schedule
     - Daily at 03:00 UTC → Step Function
"""

# ── Variables ────────────────────────────────────────────────────────────────

variable "iceberg_bucket" {
  description = "S3 bucket where all Iceberg table data lives"
  type        = string
}

variable "scripts_bucket" {
  description = "S3 bucket where Glue scripts are uploaded"
  type        = string
}

variable "raw_database" {
  default = "iceberg_raw_db"
}

variable "ops_database" {
  default = "operational_data_store"
}

variable "snapshot_retention_days" {
  default = "7"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}

# ── IAM — Shared Glue Role ───────────────────────────────────────────────────

resource "aws_iam_role" "glue_maintenance" {
  name = "lean-ops-glue-maintenance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_maintenance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_maintenance_permissions" {
  name = "GlueMaintenance"
  role = aws_iam_role.glue_maintenance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3IcebergAndScripts"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::${var.iceberg_bucket}",  "arn:aws:s3:::${var.iceberg_bucket}/*",
          "arn:aws:s3:::${var.scripts_bucket}",  "arn:aws:s3:::${var.scripts_bucket}/*"
        ]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = ["glue:*"]
        Resource = "*"
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

# ── Glue Job: ODS Setup (one-time) ───────────────────────────────────────────

resource "aws_glue_job" "setup_ops_tables" {
  name         = "lean-ops-setup-ops-tables"
  description  = "One-time: creates operational_data_store tables in Iceberg if they don't exist."
  role_arn     = aws_iam_role.glue_maintenance.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue_v2/setup/create_ops_tables.py"
    python_version  = "3"
  }

  default_arguments = {
    "--iceberg_bucket"          = var.iceberg_bucket
    "--ops_database"            = var.ops_database
    "--enable-iceberg"          = "true"
    "--datalake-formats"        = "iceberg"
    "--extra-py-files"          = "s3://${var.scripts_bucket}/glue_v2/utils.zip"
    "--TempDir"                 = "s3://${var.iceberg_bucket}/temp/glue/"
  }
}

# ── Glue Job: Table Compactor (daily) ────────────────────────────────────────

resource "aws_glue_job" "table_compactor" {
  name         = "lean-ops-table-compactor"
  description  = "Daily: compacts prior-day partitions across all active Iceberg tables and logs results."
  role_arn     = aws_iam_role.glue_maintenance.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 4
  timeout      = 120  # minutes

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/glue_v2/table_compactor.py"
    python_version  = "3"
  }

  default_arguments = {
    "--iceberg_bucket"           = var.iceberg_bucket
    "--raw_database"             = var.raw_database
    "--ops_database"             = var.ops_database
    "--snapshot_retention_days"  = var.snapshot_retention_days
    "--enable-iceberg"           = "true"
    "--datalake-formats"         = "iceberg"
    "--extra-py-files"           = "s3://${var.scripts_bucket}/glue_v2/utils.zip"
    "--TempDir"                  = "s3://${var.iceberg_bucket}/temp/glue/"
  }
}

# ── IAM — Step Function Role ─────────────────────────────────────────────────

resource "aws_iam_role" "sfn_maintenance" {
  name = "lean-ops-sfn-maintenance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "states.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "sfn_glue_invoke" {
  name = "InvokeMaintenanceGlueJobs"
  role = aws_iam_role.sfn_maintenance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
      Resource = "*"
    }]
  })
}

# ── Step Function ─────────────────────────────────────────────────────────────

resource "aws_sfn_state_machine" "table_maintenance" {
  name     = "LeanOps-TableMaintenance"
  role_arn = aws_iam_role.sfn_maintenance.arn

  definition = jsonencode({
    Comment = "Daily Iceberg table maintenance: compact + snapshot expiry across all layers."
    StartAt = "RunCompactor"
    States = {
      RunCompactor = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.table_compactor.name
        }
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "MaintenanceFailed"
          ResultPath  = "$.error"
        }]
        Next = "MaintenanceSucceeded"
      }
      MaintenanceSucceeded = {
        Type = "Succeed"
      }
      MaintenanceFailed = {
        Type  = "Fail"
        Cause = "Compaction Glue job encountered errors. Check maintenance_log for per-table results."
      }
    }
  })
}

# ── IAM — EventBridge (Scheduler) Role ───────────────────────────────────────

resource "aws_iam_role" "eventbridge_maintenance" {
  name = "lean-ops-eventbridge-maintenance-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "eventbridge_sfn" {
  name = "StartMaintenanceSFN"
  role = aws_iam_role.eventbridge_maintenance.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.table_maintenance.arn
    }]
  })
}

# ── EventBridge Schedule: Daily 03:00 UTC ────────────────────────────────────

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
  input     = jsonencode({ trigger = "scheduled", run_date = "auto" })
}
