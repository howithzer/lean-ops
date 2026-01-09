# =============================================================================
# LEAN-OPS: UNIFIED MODULE
# =============================================================================
#
# Unified dual-output processing for all topics:
# - Single Glue job writes to both Curated and Semantic
# - Independent checkpoints for failure isolation
# - Step Functions orchestration
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

variable "curated_database" {
  description = "Glue database for Curated tables"
  type        = string
  default     = "curated_db"
}

variable "semantic_database" {
  description = "Glue database for Semantic tables"
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

variable "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  type        = string
}

variable "schedule_expression" {
  description = "Schedule for unified jobs"
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

resource "aws_glue_job" "unified" {
  name     = "${local.name_prefix}-unified-job"
  role_arn = var.glue_role_arn

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${var.iceberg_bucket}/glue-scripts/unified_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--datalake-formats"                 = "iceberg"
    "--conf"                             = "spark.sql.iceberg.handle-timestamp-without-timezone=true"
    "--raw_database"                     = var.raw_database
    "--curated_database"                 = var.curated_database
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

resource "aws_sfn_state_machine" "unified_orchestrator" {
  name     = "${local.name_prefix}-unified-orchestrator"
  role_arn = aws_iam_role.step_functions_role.arn

  definition = jsonencode({
    Comment = "Unified job orchestrator with schema check and tiered outcomes"
    StartAt = "CheckSchemaExists"
    States = {
      CheckSchemaExists = {
        Type       = "Task"
        Resource   = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.check_schema.arn
          Payload = {
            "topic_name.$" = "$.topic_name"
            "schema_bucket" = var.schema_bucket
          }
        }
        ResultPath = "$.schemaCheck"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "NoSchema"
        }]
        Next = "HasSchema?"
      }
      
      "HasSchema?" = {
        Type    = "Choice"
        Choices = [{
          Variable     = "$.schemaCheck.Payload.exists"
          BooleanEquals = true
          Next         = "RunUnifiedJob"
        }]
        Default = "NoSchema"
      }
      
      NoSchema = {
        Type    = "Succeed"
        Comment = "No schema file found - topic not ready"
      }
      
      RunUnifiedJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.unified.name
          Arguments = {
            "--topic_name.$" = "$.topic_name"
          }
        }
        ResultPath = "$.glueResult"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          Next        = "HandleJobFailure"
        }]
        Next = "CheckJobResult"
      }
      
      CheckJobResult = {
        Type    = "Choice"
        Choices = [
          {
            Variable     = "$.glueResult.JobRunState"
            StringEquals = "SUCCEEDED"
            Next         = "FullSuccess"
          }
        ]
        Default = "CuratedOnlySuccess"
      }
      
      HandleJobFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn   = aws_sns_topic.alerts.arn
          Message.$  = "States.Format('Unified job failed for topic {}', $.topic_name)"
          Subject    = "Lean-Ops: Unified Job Failed"
        }
        Next = "Fail"
      }
      
      FullSuccess = {
        Type = "Succeed"
        Comment = "Both Curated and Semantic succeeded"
      }
      
      CuratedOnlySuccess = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn  = aws_sns_topic.alerts.arn
          Message.$ = "States.Format('Semantic failed for topic {}, Curated succeeded', $.topic_name)"
          Subject   = "Lean-Ops: Semantic Failed (Curated OK)"
        }
        Next = "SucceedWithWarning"
      }
      
      SucceedWithWarning = {
        Type = "Succeed"
        Comment = "Curated succeeded, Semantic failed"
      }
      
      Fail = { Type = "Fail" }
    }
  })

  tags = local.common_tags
}

# =============================================================================
# CHECK SCHEMA LAMBDA
# =============================================================================

data "archive_file" "check_schema" {
  type        = "zip"
  output_path = "${path.module}/.build/check_schema.zip"
  
  source {
    content  = <<-EOF
import boto3
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = event['schema_bucket']
    key = f"{event['topic_name']}_semantic_schema.json"
    
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return {'exists': True, 'key': key}
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return {'exists': False, 'key': key}
        raise
EOF
    filename = "handler.py"
  }
}

resource "aws_lambda_function" "check_schema" {
  filename         = data.archive_file.check_schema.output_path
  function_name    = "${local.name_prefix}-check-schema"
  role             = aws_iam_role.lambda_role.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.check_schema.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30

  tags = local.common_tags
}

# =============================================================================
# EVENTBRIDGE SCHEDULES (Per Topic)
# =============================================================================

resource "aws_cloudwatch_event_rule" "unified_schedule" {
  for_each = toset(var.topics)
  
  name                = "${local.name_prefix}-unified-${each.key}"
  schedule_expression = var.schedule_expression
  
  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "unified_target" {
  for_each = toset(var.topics)
  
  rule     = aws_cloudwatch_event_rule.unified_schedule[each.key].name
  arn      = aws_sfn_state_machine.unified_orchestrator.arn
  role_arn = aws_iam_role.eventbridge_role.arn
  
  input = jsonencode({
    topic_name = each.key
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
# IAM: Lambda
# =============================================================================

resource "aws_iam_role" "lambda_role" {
  name = "${local.name_prefix}-check-schema-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${local.name_prefix}-check-schema-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:HeadObject"]
        Resource = "arn:aws:s3:::${var.schema_bucket}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# =============================================================================
# IAM: Step Functions
# =============================================================================

resource "aws_iam_role" "step_functions_role" {
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

resource "aws_iam_role_policy" "step_functions_policy" {
  name = "${local.name_prefix}-unified-sfn-policy"
  role = aws_iam_role.step_functions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
        Resource = aws_glue_job.unified.arn
      },
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.check_schema.arn
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

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${local.name_prefix}-unified-eb-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "states:StartExecution"
      Resource = aws_sfn_state_machine.unified_orchestrator.arn
    }]
  })
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "glue_job_name" {
  value = aws_glue_job.unified.name
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.unified_orchestrator.arn
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
