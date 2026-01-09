# =============================================================================
# LEAN-OPS: ROOT MODULE
# =============================================================================
#
# Simplified, fail-aware data pipeline with:
# - 2 layers (RAW → Semantic)
# - Centralized error management
# - Full observability
#
# Usage:
#   terraform init
#   terraform plan -var-file="environments/dev.tfvars"
#   terraform apply -var-file="environments/dev.tfvars"
#
# =============================================================================

terraform {
  required_version = ">= 1.0"
  
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
  
  # Uncomment for remote state
  # backend "s3" {
  #   bucket = "lean-ops-terraform-state"
  #   key    = "lean-ops/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region
}

# =============================================================================
# VARIABLES
# =============================================================================

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "topics" {
  description = "List of topics to process"
  type        = list(string)
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

variable "schema_bucket" {
  description = "S3 bucket for schema registry"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  type        = string
}

variable "maximum_concurrency" {
  description = "Max concurrent Lambda invocations per queue (thundering herd protection)"
  type        = number
  default     = 50
}

variable "semantic_schedule" {
  description = "Schedule for semantic jobs"
  type        = string
  default     = "rate(15 minutes)"
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  common_tags = {
    Project     = "lean-ops"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# =============================================================================
# SHARED SERVICES MODULE
# =============================================================================

module "shared_services" {
  source      = "./modules/shared-services"
  environment = var.environment
  tags        = local.common_tags
}

# =============================================================================
# INGESTION MODULE
# =============================================================================

module "ingestion" {
  source       = "./modules/ingestion"
  environment  = var.environment
  topics       = var.topics
  iceberg_bucket = var.iceberg_bucket
  
  error_tracker_table_name = module.shared_services.error_tracker_table_name
  maximum_concurrency      = var.maximum_concurrency
  
  tags = local.common_tags
  
  depends_on = [module.shared_services]
}

# =============================================================================
# UNIFIED MODULE (replaces semantic module)
# =============================================================================

module "unified" {
  source       = "./modules/unified"
  environment  = var.environment
  topics       = var.topics
  iceberg_bucket = var.iceberg_bucket
  schema_bucket  = var.schema_bucket
  glue_role_arn  = var.glue_role_arn
  
  checkpoints_table_name = module.shared_services.checkpoints_table_name
  schedule_expression    = var.semantic_schedule
  
  tags = local.common_tags
  
  depends_on = [module.shared_services, module.ingestion]
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "sqs_queue_urls" {
  description = "SQS queue URLs by topic"
  value       = module.ingestion.sqs_queue_urls
}

output "centralized_dlq_url" {
  description = "Centralized DLQ URL"
  value       = module.ingestion.centralized_dlq_url
}

output "firehose_stream_name" {
  description = "Shared Firehose stream name"
  value       = module.ingestion.firehose_stream_name
}

output "state_machine_arn" {
  description = "Unified orchestrator Step Function ARN"
  value       = module.unified.state_machine_arn
}

output "alerts_topic_arn" {
  description = "SNS topic for alerts"
  value       = module.unified.alerts_topic_arn
}

output "dynamodb_tables" {
  description = "DynamoDB table names"
  value = {
    checkpoints        = module.shared_services.checkpoints_table_name
    error_tracker      = module.shared_services.error_tracker_table_name
    counters           = module.shared_services.counters_table_name
    topic_registry     = module.shared_services.topic_registry_table_name
    compaction_tracking = module.shared_services.compaction_tracking_table_name
    locks              = module.shared_services.locks_table_name
  }
}
