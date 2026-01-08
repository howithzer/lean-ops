# =============================================================================
# LEAN-OPS: SHARED SERVICES MODULE
# =============================================================================
#
# Central DynamoDB tables for:
# - Checkpoints (last processed snapshot per topic)
# - Error Tracker (all errors with context)
# - Counters (daily metrics per topic)
# - Topic Registry (configuration per topic)
# - Compaction Tracking (maintenance history)
# - Locks (duplicate trigger prevention)
#
# =============================================================================

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

locals {
  name_prefix = "lean-ops-${var.environment}"
  
  common_tags = merge(var.tags, {
    Project     = "lean-ops"
    Environment = var.environment
    ManagedBy   = "terraform"
  })
}

# =============================================================================
# CHECKPOINTS TABLE
# =============================================================================

resource "aws_dynamodb_table" "checkpoints" {
  name         = "${local.name_prefix}-checkpoints"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "topic_name"

  attribute {
    name = "topic_name"
    type = "S"
  }

  tags = local.common_tags
}

# =============================================================================
# ERROR TRACKER TABLE
# =============================================================================

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

  # GSI for querying by error_type
  global_secondary_index {
    name            = "error-type-index"
    hash_key        = "error_type"
    projection_type = "ALL"
  }

  attribute {
    name = "error_type"
    type = "S"
  }

  # TTL for automatic cleanup (30 days)
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = local.common_tags
}

# =============================================================================
# COUNTERS TABLE
# =============================================================================

resource "aws_dynamodb_table" "counters" {
  name         = "${local.name_prefix}-counters"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "topic_name"
  range_key    = "date"

  attribute {
    name = "topic_name"
    type = "S"
  }

  attribute {
    name = "date"
    type = "S"
  }

  # TTL for automatic cleanup (90 days)
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = local.common_tags
}

# =============================================================================
# TOPIC REGISTRY TABLE
# =============================================================================

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

# =============================================================================
# COMPACTION TRACKING TABLE
# =============================================================================

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
# LOCKS TABLE (Duplicate Prevention)
# =============================================================================

resource "aws_dynamodb_table" "locks" {
  name         = "${local.name_prefix}-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "topic_name"

  attribute {
    name = "topic_name"
    type = "S"
  }

  # TTL for automatic lock cleanup (1 hour)
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  tags = local.common_tags
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "checkpoints_table_name" {
  value = aws_dynamodb_table.checkpoints.name
}

output "checkpoints_table_arn" {
  value = aws_dynamodb_table.checkpoints.arn
}

output "error_tracker_table_name" {
  value = aws_dynamodb_table.error_tracker.name
}

output "error_tracker_table_arn" {
  value = aws_dynamodb_table.error_tracker.arn
}

output "counters_table_name" {
  value = aws_dynamodb_table.counters.name
}

output "topic_registry_table_name" {
  value = aws_dynamodb_table.topic_registry.name
}

output "compaction_tracking_table_name" {
  value = aws_dynamodb_table.compaction_tracking.name
}

output "locks_table_name" {
  value = aws_dynamodb_table.locks.name
}
