# =============================================================================
# INGESTION MODULE - Firehose Delivery Stream
# =============================================================================
# This module creates the Kinesis Firehose delivery stream that writes
# to Iceberg tables via multi-table destination configuration.
# =============================================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.9"
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

variable "database_name" {
  description = "Glue database name"
  type        = string
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

variable "firehose_transform_arn" {
  description = "Firehose transform Lambda ARN"
  type        = string
}

variable "buffering_size" {
  description = "Firehose buffering size in MB"
  type        = number
  default     = 128
}

variable "buffering_interval" {
  description = "Firehose buffering interval in seconds"
  type        = number
  default     = 60
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
    Module = "ingestion"
  })
}

data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# =============================================================================
# IAM ROLE FOR FIREHOSE
# =============================================================================

resource "aws_iam_role" "firehose" {
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

resource "aws_iam_role_policy" "firehose" {
  name = "${local.name_prefix}-firehose-policy"
  role = aws_iam_role.firehose.id

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
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.database_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.database_name}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "${var.firehose_transform_arn}:*"
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# Wait for IAM propagation
resource "time_sleep" "wait_for_iam" {
  depends_on = [
    aws_iam_role.firehose,
    aws_iam_role_policy.firehose
  ]

  create_duration = "30s"
}

# =============================================================================
# CLOUDWATCH LOG GROUP
# =============================================================================

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.name_prefix}-shared-to-iceberg"
  retention_in_days = 7
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_stream" "firehose_iceberg" {
  name           = "IcebergDelivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

resource "aws_cloudwatch_log_stream" "firehose_s3_backup" {
  name           = "S3Backup"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# =============================================================================
# FIREHOSE DELIVERY STREAM
# =============================================================================

resource "aws_kinesis_firehose_delivery_stream" "shared" {
  name        = "${local.name_prefix}-shared-to-iceberg"
  destination = "iceberg"

  iceberg_configuration {
    role_arn    = aws_iam_role.firehose.arn
    catalog_arn = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog"

    buffering_size     = var.buffering_size
    buffering_interval = var.buffering_interval

    s3_configuration {
      role_arn           = aws_iam_role.firehose.arn
      bucket_arn         = "arn:aws:s3:::${var.iceberg_bucket}"
      prefix             = "firehose-backup/"
      error_output_prefix = "firehose-errors/"
      buffering_size     = 64
      buffering_interval = 60

      cloudwatch_logging_options {
        enabled         = true
        log_group_name  = aws_cloudwatch_log_group.firehose.name
        log_stream_name = aws_cloudwatch_log_stream.firehose_s3_backup.name
      }
    }

    processing_configuration {
      enabled = true
      processors {
        type = "Lambda"
        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${var.firehose_transform_arn}:$LATEST"
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

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_iceberg.name
    }

    destination_table_configuration {
      database_name          = var.database_name
      table_name             = "default_staging"
      s3_error_output_prefix = "firehose-errors/table-errors/"
    }
  }

  depends_on = [
    time_sleep.wait_for_iam,
    aws_iam_role_policy.firehose
  ]

  tags = local.common_tags
}

# Permission for Firehose to invoke transform Lambda
resource "aws_lambda_permission" "firehose_invoke" {
  statement_id  = "AllowFirehoseInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.firehose_transform_arn
  principal     = "firehose.amazonaws.com"
  source_arn    = aws_kinesis_firehose_delivery_stream.shared.arn
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "stream_name" {
  description = "Firehose stream name"
  value       = aws_kinesis_firehose_delivery_stream.shared.name
}

output "stream_arn" {
  description = "Firehose stream ARN"
  value       = aws_kinesis_firehose_delivery_stream.shared.arn
}
