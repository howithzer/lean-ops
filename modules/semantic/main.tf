# =============================================================================
# LEAN-OPS: SEMANTIC MODULE
# =============================================================================
#
# Unified semantic processing for all topics:
# - Glue job (dedup + parse + PII + CDE + drift + MERGE)
# - Step Functions orchestration
# - No-data short-circuit
# - Duplicate trigger prevention (locks)
# - EventBridge scheduling
#
# =============================================================================

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "topics" {
  description = "List of topics to process"
  type        = list(string)
}

variable "raw_database" {
  description = "Glue database for RAW tables"
  type        = string
  default     = "iceberg_raw_db"
}

variable "semantic_database" {
  description = "Glue database for semantic tables"
  type        = string
  default     = "semantic_db"
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

variable "schema_bucket" {
  description = "S3 bucket for schema registry"
  type        = string
}

variable "checkpoints_table_name" {
  description = "DynamoDB table for checkpoints"
  type        = string
}

variable "locks_table_name" {
  description = "DynamoDB table for locks"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  type        = string
}

variable "schedule_expression" {
  description = "Schedule for semantic jobs"
  type        = string
  default     = "rate(15 minutes)"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  name_prefix = "lean-ops-${var.environment}"
  
  common_tags = merge(var.tags, {
    Project     = "lean-ops"
    Environment = var.environment
    ManagedBy   = "terraform"
  })
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# =============================================================================
# GLUE JOB
# =============================================================================

resource "aws_glue_job" "semantic" {
  name     = "${local.name_prefix}-unified-semantic"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${var.iceberg_bucket}/glue-scripts/semantic_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.iceberg.handle-timestamp-without-timezone=true"
    "--raw_database"                     = var.raw_database
    "--semantic_database"                = var.semantic_database
    "--schema_bucket"                    = var.schema_bucket
    "--iceberg_warehouse"                = "s3://${var.iceberg_bucket}"
    "--checkpoints_table"                = var.checkpoints_table_name
  }

  tags = local.common_tags
}

# =============================================================================
# STEP FUNCTIONS
# =============================================================================

resource "aws_sfn_state_machine" "semantic_orchestrator" {
  name     = "${local.name_prefix}-semantic-orchestrator"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Semantic job orchestrator with no-data short-circuit and lock"
    StartAt = "AcquireLock"
    States = {
      AcquireLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"
        Parameters = {
          TableName = var.locks_table_name
          Item = {
            topic_name   = { "S.$" = "$.topic_name" }
            locked_at    = { "S.$" = "$$.State.EnteredTime" }
            execution_id = { "S.$" = "$$.Execution.Id" }
            ttl          = { "N" = "3600" }
          }
          ConditionExpression = "attribute_not_exists(topic_name)"
        }
        ResultPath = "$.lock"
        Catch = [{
          ErrorEquals = ["DynamoDB.ConditionalCheckFailedException"]
          Next        = "AlreadyRunning"
        }]
        Next = "GetCheckpoint"
      }
      
      AlreadyRunning = {
        Type    = "Succeed"
        Comment = "Another execution is running, exit gracefully"
      }
      
      GetCheckpoint = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:getItem"
        Parameters = {
          TableName = var.checkpoints_table_name
          Key       = { topic_name = { "S.$" = "$.topic_name" } }
        }
        ResultPath = "$.checkpoint"
        Next       = "GetCurrentSnapshot"
      }
      
      GetCurrentSnapshot = {
        Type     = "Task"
        Resource = "arn:aws:states:::athena:startQueryExecution.sync"
        Parameters = {
          QueryString.$              = "States.Format('SELECT snapshot_id FROM \"{}\".\"{}$snapshots\" ORDER BY committed_at DESC LIMIT 1', $.raw_database, States.Format('{}_staging', $.topic_name))"
          WorkGroup                  = "primary"
          QueryExecutionContext      = { Database = var.raw_database }
          ResultConfiguration = {
            OutputLocation = "s3://${var.iceberg_bucket}/athena-results/"
          }
        }
        ResultPath = "$.currentSnapshot"
        Next       = "CheckNewData"
      }
      
      CheckNewData = {
        Type    = "Choice"
        Choices = [{
          Variable         = "$.currentSnapshot.QueryExecution.Status.State"
          StringEquals     = "SUCCEEDED"
          Next             = "RunSemanticJob"
        }]
        Default = "NoDataExit"
      }
      
      NoDataExit = {
        Type    = "Task"
        Resource = "arn:aws:states:::dynamodb:deleteItem"
        Parameters = {
          TableName = var.locks_table_name
          Key       = { topic_name = { "S.$" = "$.topic_name" } }
        }
        Next = "NoDataSuccess"
      }
      
      NoDataSuccess = {
        Type    = "Succeed"
        Comment = "No new data, exit after releasing lock"
      }
      
      RunSemanticJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.semantic.name
          Arguments = {
            "--topic_name.$"     = "$.topic_name"
            "--snapshot_after.$" = "States.Format('{}', $.checkpoint.Item.last_snapshot.S)"
          }
        }
        ResultPath = "$.glueResult"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "HandleJobFailure"
        }]
        Next = "ReleaseLock"
      }
      
      HandleJobFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn   = aws_sns_topic.alerts.arn
          Message.$  = "States.Format('Semantic job failed for topic {}', $.topic_name)"
          Subject    = "Lean-Ops: Semantic Job Failed"
        }
        ResultPath = "$.alertResult"
        Next       = "ReleaseLockOnFailure"
      }
      
      ReleaseLockOnFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:deleteItem"
        Parameters = {
          TableName = var.locks_table_name
          Key       = { topic_name = { "S.$" = "$.topic_name" } }
        }
        Next = "Fail"
      }
      
      Fail = { Type = "Fail" }
      
      ReleaseLock = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:deleteItem"
        Parameters = {
          TableName = var.locks_table_name
          Key       = { topic_name = { "S.$" = "$.topic_name" } }
        }
        Next = "Success"
      }
      
      Success = { Type = "Succeed" }
    }
  })

  tags = local.common_tags
}

# =============================================================================
# EVENTBRIDGE SCHEDULES (Per Topic)
# =============================================================================

resource "aws_cloudwatch_event_rule" "semantic_schedule" {
  for_each = toset(var.topics)
  
  name                = "${local.name_prefix}-semantic-${each.key}"
  schedule_expression = var.schedule_expression
  
  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "semantic_target" {
  for_each = toset(var.topics)
  
  rule     = aws_cloudwatch_event_rule.semantic_schedule[each.key].name
  arn      = aws_sfn_state_machine.semantic_orchestrator.arn
  role_arn = aws_iam_role.eventbridge_role.arn
  
  input = jsonencode({
    topic_name   = each.key
    raw_database = var.raw_database
  })
}

# =============================================================================
# SNS ALERTS
# =============================================================================

resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
  tags = local.common_tags
}

# =============================================================================
# IAM: Step Functions
# =============================================================================

resource "aws_iam_role" "step_functions_role" {
  name = "${local.name_prefix}-step-functions-role"

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

resource "aws_iam_role_policy" "step_functions_policy" {
  name = "${local.name_prefix}-step-functions-policy"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
        Resource = aws_glue_job.semantic.arn
      },
      {
        Effect = "Allow"
        Action = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem"]
        Resource = [
          "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.checkpoints_table_name}",
          "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.locks_table_name}"
        ]
      },
      {
        Effect = "Allow"
        Action = ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:aws:s3:::${var.iceberg_bucket}/athena-results/*"
      },
      {
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

# =============================================================================
# IAM: EventBridge
# =============================================================================

resource "aws_iam_role" "eventbridge_role" {
  name = "${local.name_prefix}-eventbridge-role"

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

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${local.name_prefix}-eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.semantic_orchestrator.arn
    }]
  })
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "glue_job_name" {
  value = aws_glue_job.semantic.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.semantic_orchestrator.arn
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
