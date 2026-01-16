# =============================================================================
# ORCHESTRATION MODULE - Step Functions and EventBridge
# =============================================================================
# This module creates:
# - Step Functions state machine for unified processing orchestration
# - EventBridge rules for scheduled execution per topic
# - Glue job for unified processing
# =============================================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# =============================================================================
# VARIABLES
# =============================================================================

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "lean-ops"
}

variable "topics" {
  description = "List of topics to orchestrate"
  type        = list(string)
}

variable "schedule_expression" {
  description = "Schedule expression for orchestration"
  type        = string
  default     = "rate(15 minutes)"
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  type        = string
}

variable "check_schema_lambda_arn" {
  description = "Check schema Lambda ARN"
  type        = string
}

variable "ensure_curated_table_lambda_arn" {
  description = "Ensure curated table Lambda ARN"
  type        = string
}

variable "schema_bucket" {
  description = "S3 bucket for schema files"
  type        = string
}

variable "alerts_topic_arn" {
  description = "SNS topic ARN for alerts"
  type        = string
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  common_tags = merge(var.tags, {
    Module = "orchestration"
  })
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# =============================================================================
# GLUE JOB
# =============================================================================

resource "aws_glue_job" "unified" {
  name     = "${local.name_prefix}-unified-job"
  role_arn = var.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.iceberg_bucket}/glue-scripts/curated_processor.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${var.iceberg_bucket}/glue-temp/spark-logs/"
    "--TempDir"                          = "s3://${var.iceberg_bucket}/glue-temp/"
    "--datalake-formats"                 = "iceberg"
    # Extra Python files: utils/ package for modular code
    "--extra-py-files"                   = "s3://${var.iceberg_bucket}/glue-scripts/glue_libs.zip"
    # Key: IcebergSparkSessionExtensions enables MERGE/UPDATE/DELETE
    "--conf"                             = "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.iceberg.handle-timestamp-without-timezone=true"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60

  tags = local.common_tags
}

# =============================================================================
# STEP FUNCTIONS
# =============================================================================

resource "aws_iam_role" "step_functions" {
  name = "${local.name_prefix}-unified-sfn-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "states.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_sfn_state_machine" "unified_orchestrator" {
  name     = "${local.name_prefix}-unified-orchestrator"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "Unified Orchestrator with Schema Gate - Curated + Semantic processing"
    StartAt = "CheckSchemaExists"
    States = {
      # Step 1: Check if schema file exists in S3
      CheckSchemaExists = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.check_schema_lambda_arn
          Payload = {
            "bucket"       = var.schema_bucket
            "key.$"        = "States.Format('schemas/{}.json', $.topic_name)"
          }
        }
        ResultSelector = {
          "exists.$" = "$.Payload.exists"
        }
        ResultPath = "$.schemaCheck"
        Next       = "SchemaGate"
      }
      # Step 2: Schema Gate - skip if no schema
      SchemaGate = {
        Type    = "Choice"
        Choices = [{
          Variable      = "$.schemaCheck.exists"
          BooleanEquals = true
          Next          = "EnsureCuratedTable"
        }]
        Default = "SkipNoSchema"
      }
      # Step 2a: Skip if no schema found
      SkipNoSchema = {
        Type   = "Pass"
        Result = {
          status = "skipped"
          reason = "Schema file not found - topic not ready for curation"
        }
        ResultPath = "$.skipReason"
        End        = true
      }
      # Step 3: Ensure Curated table exists with proper DDL
      EnsureCuratedTable = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = var.ensure_curated_table_lambda_arn
          Payload = {
            "database"      = "iceberg_curated_db"
            "table.$"       = "$.topic_name"
            "schema_bucket" = var.schema_bucket
            "schema_key.$"  = "States.Format('schemas/{}.json', $.topic_name)"
            "iceberg_bucket" = var.iceberg_bucket
          }
        }
        ResultSelector = {
          "status.$"        = "$.Payload.status"
          "table.$"         = "$.Payload.table"
          "columns_count.$" = "$.Payload.columns_count"
        }
        ResultPath = "$.tableCheck"
        Next       = "RunCurated"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "HandleError"
        }]
      }
      # Step 4: Run Curation Glue Job
      RunCurated = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName   = aws_glue_job.unified.name
          Arguments = {
            "--topic_name.$"      = "$.topic_name"
            "--raw_database"      = "iceberg_raw_db"
            "--curated_database"  = "iceberg_curated_db"
            "--checkpoint_table"  = "${local.name_prefix}-checkpoints"
            "--iceberg_bucket"    = var.iceberg_bucket
          }
        }
        ResultPath = "$.glueResult"
        Next       = "CheckSemanticReady"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "HandleError"
        }]
      }
      # Step 5: Check if Semantic processing is ready
      CheckSemanticReady = {
        Type   = "Pass"
        Result = { semantic_ready = false }
        ResultPath = "$.semanticCheck"
        Comment = "Placeholder - Semantic layer not yet implemented"
        Next    = "SuccessCurationOnly"
      }
      # Success states
      SuccessCurationOnly = {
        Type    = "Succeed"
        Comment = "Curated succeeded, Semantic not yet implemented"
      }
      # Error handling
      HandleError = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = var.alerts_topic_arn
          Message  = {
            "error.$"      = "$.error"
            "topic_name.$" = "$.topic_name"
          }
        }
        Next = "Fail"
      }
      Fail = {
        Type = "Fail"
      }
    }
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "step_functions" {
  name = "${local.name_prefix}-unified-sfn-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["lambda:InvokeFunction"]
        Resource = [
          var.check_schema_lambda_arn,
          var.ensure_curated_table_lambda_arn
        ]
      },
      {
        Effect = "Allow"
        Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"]
        Resource = aws_glue_job.unified.arn
      },
      {
        Effect = "Allow"
        Action = ["sns:Publish"]
        Resource = var.alerts_topic_arn
      }
    ]
  })
}

# =============================================================================
# EVENTBRIDGE SCHEDULED RULES
# =============================================================================

resource "aws_iam_role" "eventbridge" {
  name = "${local.name_prefix}-unified-eb-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "eventbridge" {
  name = "${local.name_prefix}-unified-eb-policy"
  role = aws_iam_role.eventbridge.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = aws_sfn_state_machine.unified_orchestrator.arn
    }]
  })
}

resource "aws_cloudwatch_event_rule" "unified_schedule" {
  for_each = toset(var.topics)

  name                = "${local.name_prefix}-unified-${each.key}"
  schedule_expression = var.schedule_expression

  tags = merge(local.common_tags, {
    Topic = each.key
  })
}

resource "aws_cloudwatch_event_target" "unified_target" {
  for_each = toset(var.topics)

  rule     = aws_cloudwatch_event_rule.unified_schedule[each.key].name
  arn      = aws_sfn_state_machine.unified_orchestrator.arn
  role_arn = aws_iam_role.eventbridge.arn

  input = jsonencode({
    topic_name = each.key
  })
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = aws_sfn_state_machine.unified_orchestrator.arn
}

output "state_machine_name" {
  description = "Step Functions state machine name"
  value       = aws_sfn_state_machine.unified_orchestrator.name
}

output "glue_job_name" {
  description = "Glue job name"
  value       = aws_glue_job.unified.name
}
