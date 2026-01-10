# =============================================================================
# OBSERVABILITY MODULE - CloudWatch Alarms and SNS Alerts
# =============================================================================
# This module creates:
# - SNS topic for alerts
# - CloudWatch alarms for DLQ, Lambda errors, etc.
# - Circuit breaker infrastructure
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

variable "dlq_name" {
  description = "DLQ name for alarm"
  type        = string
}

variable "sqs_processor_function_name" {
  description = "SQS processor Lambda function name for error rate alarm"
  type        = string
  default     = ""
}

variable "circuit_breaker_lambda_arn" {
  description = "Circuit breaker Lambda ARN for SNS subscription"
  type        = string
  default     = ""
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
    Module = "observability"
  })
}

# =============================================================================
# SNS TOPIC FOR ALERTS
# =============================================================================

resource "aws_sns_topic" "alerts" {
  name = "${local.name_prefix}-alerts"
  tags = local.common_tags
}

# =============================================================================
# CLOUDWATCH ALARMS
# =============================================================================

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
  alarm_description   = "Messages in DLQ indicate processing failures"
  
  dimensions = {
    QueueName = var.dlq_name
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

# Lambda error rate alarm (Circuit Breaker trigger)
resource "aws_cloudwatch_metric_alarm" "lambda_error_rate" {
  alarm_name          = "${local.name_prefix}-sqs-processor-error-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  threshold           = 50
  alarm_description   = "SQS Processor error rate > 50% - triggers circuit breaker"

  metric_query {
    id          = "error_rate"
    expression  = "errors / invocations * 100"
    label       = "Error Rate (%)"
    return_data = true
  }

  metric_query {
    id = "errors"
    metric {
      metric_name = "Errors"
      namespace   = "AWS/Lambda"
      period      = 300
      stat        = "Sum"
      dimensions = {
        FunctionName = var.sqs_processor_function_name
      }
    }
  }

  metric_query {
    id = "invocations"
    metric {
      metric_name = "Invocations"
      namespace   = "AWS/Lambda"
      period      = 300
      stat        = "Sum"
      dimensions = {
        FunctionName = var.sqs_processor_function_name
      }
    }
  }

  alarm_actions = [aws_sns_topic.alerts.arn]
  ok_actions    = [aws_sns_topic.alerts.arn]

  tags = local.common_tags
}

# Circuit breaker SNS subscription
resource "aws_sns_topic_subscription" "circuit_breaker" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "lambda"
  endpoint  = var.circuit_breaker_lambda_arn
}

resource "aws_lambda_permission" "circuit_breaker_sns" {
  statement_id  = "AllowSNSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = var.circuit_breaker_lambda_arn
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.alerts.arn
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "alerts_topic_arn" {
  description = "SNS alerts topic ARN"
  value       = aws_sns_topic.alerts.arn
}

output "alerts_topic_name" {
  description = "SNS alerts topic name"
  value       = aws_sns_topic.alerts.name
}

output "dlq_alarm_arn" {
  description = "DLQ CloudWatch alarm ARN"
  value       = aws_cloudwatch_metric_alarm.dlq_not_empty.arn
}

output "error_rate_alarm_arn" {
  description = "Lambda error rate alarm ARN (for circuit breaker)"
  value       = aws_cloudwatch_metric_alarm.lambda_error_rate.arn
}

