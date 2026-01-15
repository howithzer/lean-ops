# =============================================================================
# COMPUTE MODULE - Lambda Functions
# =============================================================================
# This module creates all Lambda functions:
# - SQS Processor: Reads from SQS, sends to Firehose
# - Firehose Transform: Adds routing metadata for multi-table
# - Check Schema: Validates schema existence for semantic processing
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

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "lean-ops"
}

variable "topics" {
  description = "List of topics"
  type        = list(string)
}

variable "firehose_stream_name" {
  description = "Firehose stream name (used for env var and IAM policy)"
  type        = string
}

variable "sqs_queue_arns" {
  description = "Map of SQS queue ARNs by topic"
  type        = map(string)
}

variable "dlq_arn" {
  description = "DLQ ARN for failed messages"
  type        = string
}

variable "error_tracker_table_arn" {
  description = "Error tracker DynamoDB table ARN"
  type        = string
}

variable "error_tracker_table_name" {
  description = "Error tracker DynamoDB table name"
  type        = string
}

variable "schema_bucket" {
  description = "S3 bucket for schema files"
  type        = string
}

variable "archive_bucket" {
  description = "S3 bucket for DLQ archive (dlq-archive/ prefix)"
  type        = string
}

variable "checkpoint_table_arn" {
  description = "Checkpoint DynamoDB table ARN"
  type        = string
  default     = ""
}

variable "checkpoint_table_name" {
  description = "Checkpoint DynamoDB table name"
  type        = string
  default     = "lean-ops-checkpoints"
}

variable "raw_database" {
  description = "Glue database name for RAW tables"
  type        = string
  default     = "iceberg_raw_db"
}

variable "curated_database" {
  description = "Glue database name for Curated tables"
  type        = string
  default     = "iceberg_curated_db"
}

variable "semantic_database" {
  description = "Glue database name for Semantic tables"
  type        = string
  default     = "iceberg_semantic_db"
}

variable "tags" {
  description = "Common tags"
  type        = map(string)
  default     = {}
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  common_tags = merge(var.tags, {
    Module = "compute"
  })
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# =============================================================================
# GLUE IAM ROLE
# =============================================================================

resource "aws_iam_role" "glue" {
  name = "${local.name_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

# Attach AWS managed Glue service policy (required for Glue job execution)
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue" {
  name = "${local.name_prefix}-glue-policy"
  role = aws_iam_role.glue.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.iceberg_bucket}",
          "arn:aws:s3:::${var.iceberg_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetTableVersion",
          "glue:GetTableVersions",
          "glue:UpdateTable",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/*/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem"
        ]
        Resource = var.checkpoint_table_arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

output "glue_role_arn" {
  description = "Glue IAM role ARN"
  value       = aws_iam_role.glue.arn
}

# =============================================================================
# LAMBDA: SQS PROCESSOR
# =============================================================================

data "archive_file" "sqs_processor" {
  type        = "zip"
  source_dir  = "${path.module}/.build/sqs_processor"
  output_path = "${path.module}/.build/sqs_processor.zip"
}

resource "aws_iam_role" "sqs_processor" {
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

resource "aws_iam_role_policy" "sqs_processor" {
  name = "${local.name_prefix}-sqs-processor-policy"
  role = aws_iam_role.sqs_processor.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = values(var.sqs_queue_arns)
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = var.dlq_arn
      },
      {
        Effect   = "Allow"
        Action   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
        Resource = "arn:aws:firehose:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:deliverystream/${var.firehose_stream_name}"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = var.error_tracker_table_arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_lambda_function" "sqs_processor" {
  filename         = data.archive_file.sqs_processor.output_path
  function_name    = "${local.name_prefix}-sqs-processor"
  role             = aws_iam_role.sqs_processor.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.sqs_processor.output_base64sha256
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      FIREHOSE_STREAM_NAME = var.firehose_stream_name
      ERROR_TABLE          = var.error_tracker_table_name
    }
  }

  tags = local.common_tags
}

# =============================================================================
# LAMBDA: FIREHOSE TRANSFORM
# =============================================================================

data "archive_file" "firehose_transform" {
  type        = "zip"
  source_dir  = "${path.module}/.build/firehose_transform"
  output_path = "${path.module}/.build/firehose_transform.zip"
}

resource "aws_iam_role" "firehose_transform" {
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

resource "aws_iam_role_policy" "firehose_transform" {
  name = "${local.name_prefix}-firehose-transform-policy"
  role = aws_iam_role.firehose_transform.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
      Resource = "arn:aws:logs:*:*:*"
    }]
  })
}

resource "aws_lambda_function" "firehose_transform" {
  filename         = data.archive_file.firehose_transform.output_path
  function_name    = "${local.name_prefix}-firehose-transform"
  role             = aws_iam_role.firehose_transform.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.firehose_transform.output_base64sha256
  runtime          = "python3.11"
  timeout          = 60
  memory_size      = 256

  tags = local.common_tags
}

# =============================================================================
# LAMBDA: CHECK SCHEMA
# =============================================================================

data "archive_file" "check_schema" {
  type        = "zip"
  source_dir  = "${path.module}/.build/check_schema"
  output_path = "${path.module}/.build/check_schema.zip"
}

resource "aws_iam_role" "check_schema" {
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

resource "aws_iam_role_policy" "check_schema" {
  name = "${local.name_prefix}-check-schema-policy"
  role = aws_iam_role.check_schema.id

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

resource "aws_lambda_function" "check_schema" {
  filename         = data.archive_file.check_schema.output_path
  function_name    = "${local.name_prefix}-check-schema"
  role             = aws_iam_role.check_schema.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.check_schema.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      SCHEMA_BUCKET = var.schema_bucket
      SCHEMA_PREFIX = "schemas/"
    }
  }

  tags = local.common_tags
}

# =============================================================================
# LAMBDA: DLQ PROCESSOR
# =============================================================================
# Archives failed messages from DLQ to S3 and logs to DynamoDB

data "archive_file" "dlq_processor" {
  type        = "zip"
  source_dir  = "${path.module}/.build/dlq_processor"
  output_path = "${path.module}/.build/dlq_processor.zip"
}

resource "aws_iam_role" "dlq_processor" {
  name = "${local.name_prefix}-dlq-processor-role"

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

resource "aws_iam_role_policy" "dlq_processor" {
  name = "${local.name_prefix}-dlq-processor-policy"
  role = aws_iam_role.dlq_processor.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect   = "Allow"
        Action   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
        Resource = var.dlq_arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:PutObject"]
        Resource = "arn:aws:s3:::${var.archive_bucket}/dlq-archive/*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem"]
        Resource = var.error_tracker_table_arn
      }
    ]
  })
}

resource "aws_lambda_function" "dlq_processor" {
  filename         = data.archive_file.dlq_processor.output_path
  function_name    = "${local.name_prefix}-dlq-processor"
  role             = aws_iam_role.dlq_processor.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.dlq_processor.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 256

  environment {
    variables = {
      ARCHIVE_BUCKET = var.archive_bucket
      ERROR_TABLE    = var.error_tracker_table_name
    }
  }

  tags = local.common_tags
}

# DLQ Event Source Mapping
resource "aws_lambda_event_source_mapping" "dlq_trigger" {
  event_source_arn = var.dlq_arn
  function_name    = aws_lambda_function.dlq_processor.arn
  batch_size       = 10
  enabled          = true
}

# =============================================================================
# LAMBDA: CIRCUIT BREAKER
# =============================================================================
# Disables/enables SQS event source mappings based on error rate

data "archive_file" "circuit_breaker" {
  type        = "zip"
  source_dir  = "${path.module}/.build/circuit_breaker"
  output_path = "${path.module}/.build/circuit_breaker.zip"
}

resource "aws_iam_role" "circuit_breaker" {
  name = "${local.name_prefix}-circuit-breaker-role"

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

resource "aws_iam_role_policy" "circuit_breaker" {
  name = "${local.name_prefix}-circuit-breaker-policy"
  role = aws_iam_role.circuit_breaker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:ListEventSourceMappings",
          "lambda:UpdateEventSourceMapping"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_lambda_function" "circuit_breaker" {
  filename         = data.archive_file.circuit_breaker.output_path
  function_name    = "${local.name_prefix}-circuit-breaker"
  role             = aws_iam_role.circuit_breaker.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.circuit_breaker.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      SQS_PROCESSOR_FUNCTION = aws_lambda_function.sqs_processor.function_name
    }
  }

  tags = local.common_tags
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "sqs_processor_arn" {
  description = "SQS processor Lambda ARN"
  value       = aws_lambda_function.sqs_processor.arn
}

output "sqs_processor_name" {
  description = "SQS processor Lambda function name"
  value       = aws_lambda_function.sqs_processor.function_name
}

output "firehose_transform_arn" {
  description = "Firehose transform Lambda ARN"
  value       = aws_lambda_function.firehose_transform.arn
}

output "firehose_transform_name" {
  description = "Firehose transform Lambda function name"
  value       = aws_lambda_function.firehose_transform.function_name
}

output "check_schema_arn" {
  description = "Check schema Lambda ARN"
  value       = aws_lambda_function.check_schema.arn
}

output "check_schema_name" {
  description = "Check schema Lambda function name"
  value       = aws_lambda_function.check_schema.function_name
}

output "dlq_processor_arn" {
  description = "DLQ processor Lambda ARN"
  value       = aws_lambda_function.dlq_processor.arn
}

output "circuit_breaker_arn" {
  description = "Circuit breaker Lambda ARN"
  value       = aws_lambda_function.circuit_breaker.arn
}

# =============================================================================
# LAMBDA: GET ALL CHECKPOINTS
# =============================================================================
# Single Lambda for Step Function optimization - returns all checkpoints in one call

data "archive_file" "get_all_checkpoints" {
  type        = "zip"
  source_dir  = "${path.module}/.build/get_all_checkpoints"
  output_path = "${path.module}/.build/get_all_checkpoints.zip"
}

resource "aws_iam_role" "get_all_checkpoints" {
  name = "${local.name_prefix}-get-all-checkpoints-role"

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

resource "aws_iam_role_policy" "get_all_checkpoints" {
  name = "${local.name_prefix}-get-all-checkpoints-policy"
  role = aws_iam_role.get_all_checkpoints.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetTables"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:Query"
        ]
        Resource = var.checkpoint_table_arn
      }
    ]
  })
}

resource "aws_lambda_function" "get_all_checkpoints" {
  filename         = data.archive_file.get_all_checkpoints.output_path
  function_name    = "${local.name_prefix}-get-all-checkpoints"
  role             = aws_iam_role.get_all_checkpoints.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.get_all_checkpoints.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      RAW_DATABASE      = var.raw_database
      CURATED_DATABASE  = var.curated_database
      SEMANTIC_DATABASE = var.semantic_database
      CHECKPOINT_TABLE  = var.checkpoint_table_name
    }
  }

  tags = local.common_tags
}

output "get_all_checkpoints_arn" {
  description = "Get all checkpoints Lambda ARN"
  value       = aws_lambda_function.get_all_checkpoints.arn
}

output "get_all_checkpoints_name" {
  description = "Get all checkpoints Lambda function name"
  value       = aws_lambda_function.get_all_checkpoints.function_name
}

# =============================================================================
# UPDATE CHECKPOINT LAMBDA
# =============================================================================

data "archive_file" "update_checkpoint" {
  type        = "zip"
  source_dir  = "${path.module}/.build/update_checkpoint"
  output_path = "${path.module}/.build/update_checkpoint.zip"
}

resource "aws_iam_role" "update_checkpoint" {
  name = "${local.name_prefix}-update-checkpoint-role"

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

resource "aws_iam_role_policy" "update_checkpoint" {
  name = "${local.name_prefix}-update-checkpoint-policy"
  role = aws_iam_role.update_checkpoint.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:UpdateItem",
          "dynamodb:PutItem"
        ]
        Resource = var.checkpoint_table_arn
      }
    ]
  })
}

resource "aws_lambda_function" "update_checkpoint" {
  filename         = data.archive_file.update_checkpoint.output_path
  function_name    = "${local.name_prefix}-update-checkpoint"
  role             = aws_iam_role.update_checkpoint.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.update_checkpoint.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 128

  environment {
    variables = {
      CHECKPOINT_TABLE = var.checkpoint_table_name
    }
  }

  tags = local.common_tags
}

output "update_checkpoint_arn" {
  description = "Update checkpoint Lambda ARN"
  value       = aws_lambda_function.update_checkpoint.arn
}

output "update_checkpoint_name" {
  description = "Update checkpoint Lambda function name"
  value       = aws_lambda_function.update_checkpoint.function_name
}

# =============================================================================
# LAMBDA: CHECK SCHEMA EXISTS
# =============================================================================
# Checks if a schema file exists in S3 for a given topic (Schema Gate pattern)

data "archive_file" "check_schema_exists" {
  type        = "zip"
  source_dir  = "${path.module}/.build/check_schema_exists"
  output_path = "${path.module}/.build/check_schema_exists.zip"
}

resource "aws_iam_role" "check_schema_exists" {
  name = "${local.name_prefix}-check-schema-exists-role"

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

resource "aws_iam_role_policy" "check_schema_exists" {
  name = "${local.name_prefix}-check-schema-exists-policy"
  role = aws_iam_role.check_schema_exists.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:HeadObject", "s3:GetObject"]
        Resource = "arn:aws:s3:::${var.iceberg_bucket}/schemas/*"
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_lambda_function" "check_schema_exists" {
  filename         = data.archive_file.check_schema_exists.output_path
  function_name    = "${local.name_prefix}-check-schema-exists"
  role             = aws_iam_role.check_schema_exists.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.check_schema_exists.output_base64sha256
  runtime          = "python3.11"
  timeout          = 30
  memory_size      = 128

  tags = local.common_tags
}

output "check_schema_exists_arn" {
  description = "Check schema exists Lambda ARN"
  value       = aws_lambda_function.check_schema_exists.arn
}

# =============================================================================
# LAMBDA: ENSURE CURATED TABLE
# =============================================================================
# Ensures Curated table exists with proper DDL from schema file

data "archive_file" "ensure_curated_table" {
  type        = "zip"
  source_dir  = "${path.module}/.build/ensure_curated_table"
  output_path = "${path.module}/.build/ensure_curated_table.zip"
}

resource "aws_iam_role" "ensure_curated_table" {
  name = "${local.name_prefix}-ensure-curated-table-role"

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

resource "aws_iam_role_policy" "ensure_curated_table" {
  name = "${local.name_prefix}-ensure-curated-table-policy"
  role = aws_iam_role.ensure_curated_table.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:HeadObject"]
        Resource = "arn:aws:s3:::${var.iceberg_bucket}/schemas/*"
      },
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["s3:GetBucketLocation", "s3:GetObject", "s3:ListBucket", "s3:PutObject"]
        Resource = [
          "arn:aws:s3:::${var.iceberg_bucket}",
          "arn:aws:s3:::${var.iceberg_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetTable",
          "glue:GetDatabase",
          "glue:CreateTable",
          "glue:UpdateTable"
        ]
        Resource = [
          "arn:aws:glue:*:*:catalog",
          "arn:aws:glue:*:*:database/*",
          "arn:aws:glue:*:*:table/*/*"
        ]
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_lambda_function" "ensure_curated_table" {
  filename         = data.archive_file.ensure_curated_table.output_path
  function_name    = "${local.name_prefix}-ensure-curated-table"
  role             = aws_iam_role.ensure_curated_table.arn
  handler          = "handler.lambda_handler"
  source_code_hash = data.archive_file.ensure_curated_table.output_base64sha256
  runtime          = "python3.11"
  timeout          = 120
  memory_size      = 256

  environment {
    variables = {
      ATHENA_OUTPUT_BUCKET = var.iceberg_bucket
      ATHENA_WORKGROUP     = "primary"
    }
  }

  tags = local.common_tags
}

output "ensure_curated_table_arn" {
  description = "Ensure curated table Lambda ARN"
  value       = aws_lambda_function.ensure_curated_table.arn
}
