# =============================================================================
# LEAN-OPS: INGESTION MODULE
# =============================================================================
#
# Shared ingestion infrastructure for all topics:
# - Per-topic SQS queues
# - Centralized DLQ
# - Shared SQS Processor Lambda
# - Shared Firehose with Transform Lambda
# - S3 Error Bucket
#
# =============================================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
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

variable "topics" {
  description = "List of topics to create"
  type        = list(string)
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

variable "raw_database" {
  description = "Glue database for RAW tables"
  type        = string
  default     = "iceberg_raw_db"
}

variable "error_tracker_table_name" {
  description = "DynamoDB table for error tracking"
  type        = string
}

variable "maximum_concurrency" {
  description = "Max concurrent Lambda invocations per queue"
  type        = number
  default     = 50
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
# SQS QUEUES (Per Topic)
# =============================================================================

resource "aws_sqs_queue" "topic_queues" {
  for_each = toset(var.topics)
  
  name                       = "${local.name_prefix}-${each.key}-queue"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 1209600  # 14 days
  
  # All topics use centralized DLQ
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.centralized_dlq.arn
    maxReceiveCount     = 3
  })
  
  tags = merge(local.common_tags, {
    Topic = each.key
  })
}

# =============================================================================
# CENTRALIZED DLQ
# =============================================================================

resource "aws_sqs_queue" "centralized_dlq" {
  name                      = "${local.name_prefix}-centralized-dlq"
  message_retention_seconds = 1209600  # 14 days
  
  tags = local.common_tags
}

# DLQ not empty alarm
resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${local.name_prefix}-dlq-not-empty"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "DLQ has messages - investigate failures"
  
  dimensions = {
    QueueName = aws_sqs_queue.centralized_dlq.name
  }
  
  tags = local.common_tags
}

# =============================================================================
# SHARED SQS PROCESSOR LAMBDA
# =============================================================================

data "archive_file" "sqs_processor" {
  type        = "zip"
  source_dir  = "${path.module}/../../lambda/sqs_processor"
  output_path = "${path.module}/.build/sqs_processor.zip"
}

resource "aws_lambda_function" "sqs_processor" {
  filename         = data.archive_file.sqs_processor.output_path
  function_name    = "${local.name_prefix}-sqs-processor"
  role             = aws_iam_role.sqs_processor_role.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.sqs_processor.output_base64sha256
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      FIREHOSE_STREAM_NAME = aws_kinesis_firehose_delivery_stream.shared.name
      DLQ_URL              = aws_sqs_queue.centralized_dlq.url
      ERROR_TRACKER_TABLE  = var.error_tracker_table_name
    }
  }

  tags = local.common_tags
}

# SQS triggers for all topics with MaximumConcurrency
resource "aws_lambda_event_source_mapping" "sqs_triggers" {
  for_each = toset(var.topics)
  
  event_source_arn                   = aws_sqs_queue.topic_queues[each.key].arn
  function_name                      = aws_lambda_function.sqs_processor.arn
  batch_size                         = 100
  maximum_batching_window_in_seconds = 5
  
  # Thundering herd protection
  scaling_config {
    maximum_concurrency = var.maximum_concurrency
  }
  
  function_response_types = ["ReportBatchItemFailures"]
}

# =============================================================================
# FIREHOSE TRANSFORM LAMBDA
# =============================================================================

data "archive_file" "firehose_transform" {
  type        = "zip"
  source_dir  = "${path.module}/../../lambda/firehose_transform"
  output_path = "${path.module}/.build/firehose_transform.zip"
}

resource "aws_lambda_function" "firehose_transform" {
  filename         = data.archive_file.firehose_transform.output_path
  function_name    = "${local.name_prefix}-firehose-transform"
  role             = aws_iam_role.firehose_transform_role.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.firehose_transform.output_base64sha256
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      DATABASE_NAME = var.raw_database
      TABLE_SUFFIX  = "_staging"
    }
  }

  tags = local.common_tags
}

# Allow Firehose to invoke transform Lambda
resource "aws_lambda_permission" "firehose_invoke" {
  statement_id  = "AllowFirehoseInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.firehose_transform.function_name
  principal     = "firehose.amazonaws.com"
  source_arn    = aws_kinesis_firehose_delivery_stream.shared.arn
}

# =============================================================================
# SHARED FIREHOSE
# =============================================================================

resource "aws_kinesis_firehose_delivery_stream" "shared" {
  name        = "${local.name_prefix}-shared-to-iceberg"
  destination = "iceberg"

  iceberg_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    catalog_arn        = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"
    buffering_interval = 60
    buffering_size     = 1

    # Lambda transformation for dynamic routing
    processing_configuration {
      enabled = true
      processors {
        type = "Lambda"
        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.firehose_transform.arn}:$LATEST"
        }
        parameters {
          parameter_name  = "BufferSizeInMBs"
          parameter_value = "1"
        }
        parameters {
          parameter_name  = "BufferIntervalInSeconds"
          parameter_value = "60"
        }
      }
    }

    s3_configuration {
      role_arn            = aws_iam_role.firehose_role.arn
      bucket_arn          = "arn:aws:s3:::${var.iceberg_bucket}"
      buffering_interval  = 60
      buffering_size      = 1
      compression_format  = "UNCOMPRESSED"
      error_output_prefix = "firehose-errors/!{firehose:error-output-type}/"
    }

    # Default destination (overridden by Lambda for each record)
    destination_table_configuration {
      database_name          = var.raw_database
      table_name             = "default_staging"
      s3_error_output_prefix = "firehose-errors/table-errors/"
    }
  }

  tags = local.common_tags
}

# =============================================================================
# IAM: SQS Processor Lambda
# =============================================================================

resource "aws_iam_role" "sqs_processor_role" {
  name = "${local.name_prefix}-sqs-processor-role"

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

resource "aws_iam_role_policy" "sqs_processor_policy" {
  name = "${local.name_prefix}-sqs-processor-policy"
  role = aws_iam_role.sqs_processor_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = [for q in aws_sqs_queue.topic_queues : q.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = aws_sqs_queue.centralized_dlq.arn
      },
      {
        Effect   = "Allow"
        Action   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
        Resource = aws_kinesis_firehose_delivery_stream.shared.arn
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.error_tracker_table_name}"
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
# IAM: Firehose Transform Lambda
# =============================================================================

resource "aws_iam_role" "firehose_transform_role" {
  name = "${local.name_prefix}-firehose-transform-role"

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

resource "aws_iam_role_policy" "firehose_transform_policy" {
  name = "${local.name_prefix}-firehose-transform-policy"
  role = aws_iam_role.firehose_transform_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
      Resource = "arn:aws:logs:*:*:*"
    }]
  })
}

# =============================================================================
# IAM: Firehose
# =============================================================================

resource "aws_iam_role" "firehose_role" {
  name = "${local.name_prefix}-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "firehose_policy" {
  name = "${local.name_prefix}-firehose-policy"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::${var.iceberg_bucket}",
          "arn:aws:s3:::${var.iceberg_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable", "glue:GetTableVersion", "glue:GetTableVersions",
          "glue:UpdateTable", "glue:GetDatabase"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.raw_database}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.raw_database}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "${aws_lambda_function.firehose_transform.arn}:*"
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
# OUTPUTS
# =============================================================================

output "sqs_queue_urls" {
  description = "SQS queue URLs by topic"
  value       = { for k, v in aws_sqs_queue.topic_queues : k => v.url }
}

output "sqs_queue_arns" {
  description = "SQS queue ARNs by topic"
  value       = { for k, v in aws_sqs_queue.topic_queues : k => v.arn }
}

output "centralized_dlq_url" {
  description = "Centralized DLQ URL"
  value       = aws_sqs_queue.centralized_dlq.url
}

output "sqs_processor_lambda_arn" {
  description = "Shared SQS processor Lambda ARN"
  value       = aws_lambda_function.sqs_processor.arn
}

output "firehose_transform_lambda_arn" {
  description = "Firehose transform Lambda ARN"
  value       = aws_lambda_function.firehose_transform.arn
}

output "firehose_stream_name" {
  description = "Shared Firehose stream name"
  value       = aws_kinesis_firehose_delivery_stream.shared.name
}

output "firehose_stream_arn" {
  description = "Shared Firehose stream ARN"
  value       = aws_kinesis_firehose_delivery_stream.shared.arn
}
