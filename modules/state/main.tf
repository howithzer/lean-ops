# =============================================================================
# STATE MODULE - DynamoDB Tables for Pipeline State Management
# =============================================================================
# This module creates DynamoDB tables for:
# - Checkpoints (snapshot tracking for incremental processing)
# - Locks (distributed locking for concurrent safety)
# - Topic Registry (topic metadata and configuration)
# - Error Tracker (error logging and tracking)
# - Counters (metrics and counting)
# - Compaction Tracking (Iceberg compaction state)
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
  description = "Project name for resource naming"
  type        = string
  default     = "lean-ops"
}

variable "tags" {
  description = "Common tags for all resources"
  type        = map(string)
  default     = {}
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  common_tags = merge(var.tags, {
    Module = "state"
  })
}

# =============================================================================
# DYNAMODB TABLES
# =============================================================================

# Checkpoints table - tracks processing state per topic/layer
resource "aws_dynamodb_table" "checkpoints" {
  name         = "${local.name_prefix}-checkpoints"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pipeline_id"
  range_key    = "checkpoint_type"

  attribute {
    name = "pipeline_id"
    type = "S"
  }

  attribute {
    name = "checkpoint_type"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = local.common_tags
}

# Locks table - distributed locking
resource "aws_dynamodb_table" "locks" {
  name         = "${local.name_prefix}-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "topic_name"

  attribute {
    name = "topic_name"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = local.common_tags
}

# Topic registry - topic configuration and metadata
resource "aws_dynamodb_table" "topic_registry" {
  name         = "${local.name_prefix}-topic-registry"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "topic_name"

  attribute {
    name = "topic_name"
    type = "S"
  }

  tags = local.common_tags
}

# Error tracker - error logging with GSI for error type queries
resource "aws_dynamodb_table" "error_tracker" {
  name         = "${local.name_prefix}-error-tracker"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "topic_name"
  range_key    = "timestamp_message_id"

  attribute {
    name = "topic_name"
    type = "S"
  }

  attribute {
    name = "timestamp_message_id"
    type = "S"
  }

  attribute {
    name = "error_type"
    type = "S"
  }

  global_secondary_index {
    name            = "error-type-index"
    hash_key        = "error_type"
    projection_type = "ALL"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = local.common_tags
}

# Counters - metrics and counting
resource "aws_dynamodb_table" "counters" {
  name         = "${local.name_prefix}-counters"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "counter_id"

  attribute {
    name = "counter_id"
    type = "S"
  }

  tags = local.common_tags
}

# Compaction tracking - Iceberg compaction state
resource "aws_dynamodb_table" "compaction_tracking" {
  name         = "${local.name_prefix}-compaction-tracking"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "table_name"

  attribute {
    name = "table_name"
    type = "S"
  }

  tags = local.common_tags
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "checkpoints_table_name" {
  description = "Checkpoints table name"
  value       = aws_dynamodb_table.checkpoints.name
}

output "checkpoints_table_arn" {
  description = "Checkpoints table ARN"
  value       = aws_dynamodb_table.checkpoints.arn
}

output "locks_table_name" {
  description = "Locks table name"
  value       = aws_dynamodb_table.locks.name
}

output "topic_registry_table_name" {
  description = "Topic registry table name"
  value       = aws_dynamodb_table.topic_registry.name
}

output "error_tracker_table_name" {
  description = "Error tracker table name"
  value       = aws_dynamodb_table.error_tracker.name
}

output "error_tracker_table_arn" {
  description = "Error tracker table ARN"
  value       = aws_dynamodb_table.error_tracker.arn
}

output "counters_table_name" {
  description = "Counters table name"
  value       = aws_dynamodb_table.counters.name
}

output "compaction_tracking_table_name" {
  description = "Compaction tracking table name"
  value       = aws_dynamodb_table.compaction_tracking.name
}

output "all_table_names" {
  description = "Map of all table names"
  value = {
    checkpoints         = aws_dynamodb_table.checkpoints.name
    locks               = aws_dynamodb_table.locks.name
    topic_registry      = aws_dynamodb_table.topic_registry.name
    error_tracker       = aws_dynamodb_table.error_tracker.name
    counters            = aws_dynamodb_table.counters.name
    compaction_tracking = aws_dynamodb_table.compaction_tracking.name
  }
}
