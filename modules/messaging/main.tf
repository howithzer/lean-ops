# =============================================================================
# MESSAGING MODULE - SQS Queues for Event Ingestion
# =============================================================================
# This module creates:
# - Per-topic SQS queues for event ingestion
# - Centralized Dead Letter Queue (DLQ)
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
  description = "List of topics to create queues for"
  type        = list(string)
}

variable "visibility_timeout" {
  description = "Message visibility timeout in seconds"
  type        = number
  default     = 300
}

variable "message_retention" {
  description = "Message retention period in seconds"
  type        = number
  default     = 1209600 # 14 days
}

variable "max_receive_count" {
  description = "Max receives before sending to DLQ"
  type        = number
  default     = 3
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
    Module = "messaging"
  })
}

# =============================================================================
# CENTRALIZED DLQ
# =============================================================================

resource "aws_sqs_queue" "centralized_dlq" {
  name                       = "${local.name_prefix}-centralized-dlq"
  message_retention_seconds  = var.message_retention
  visibility_timeout_seconds = 300
  
  tags = merge(local.common_tags, {
    Purpose = "Dead Letter Queue"
  })
}

# =============================================================================
# PER-TOPIC QUEUES
# =============================================================================

resource "aws_sqs_queue" "topic_queues" {
  for_each = toset(var.topics)
  
  name                       = "${local.name_prefix}-${each.key}-queue"
  visibility_timeout_seconds = var.visibility_timeout
  message_retention_seconds  = var.message_retention
  
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.centralized_dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
  
  tags = merge(local.common_tags, {
    Topic = each.key
  })
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "dlq_url" {
  description = "Centralized DLQ URL"
  value       = aws_sqs_queue.centralized_dlq.url
}

output "dlq_arn" {
  description = "Centralized DLQ ARN"
  value       = aws_sqs_queue.centralized_dlq.arn
}

output "dlq_name" {
  description = "Centralized DLQ name"
  value       = aws_sqs_queue.centralized_dlq.name
}

output "queue_urls" {
  description = "Map of topic queue URLs"
  value       = { for k, v in aws_sqs_queue.topic_queues : k => v.url }
}

output "queue_arns" {
  description = "Map of topic queue ARNs"
  value       = { for k, v in aws_sqs_queue.topic_queues : k => v.arn }
}

output "queue_names" {
  description = "Map of topic queue names"
  value       = { for k, v in aws_sqs_queue.topic_queues : k => v.name }
}
