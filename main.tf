# =============================================================================
# LEAN-OPS ROOT MODULE
# =============================================================================
# This root module composes all granular modules in the correct dependency order:
#
# Dependency Chain:
#   storage → catalog ────┐
#             state ──────┼→ compute → ingestion ──┐
#             messaging ──┘                        │
#                                                  ↓
#             observability → orchestration ←──────┘
# =============================================================================

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
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
  description = "List of topics to deploy"
  type        = list(string)
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

variable "schema_bucket" {
  description = "S3 bucket for schema files"
  type        = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue jobs"
  type        = string
}

variable "maximum_concurrency" {
  description = "Maximum Lambda concurrency"
  type        = number
  default     = 50
}

variable "semantic_schedule" {
  description = "Schedule expression for semantic processing"
  type        = string
  default     = "rate(15 minutes)"
}

# =============================================================================
# LOCALS
# =============================================================================

locals {
  project_name = "lean-ops"
  name_prefix  = "${local.project_name}-${var.environment}"
  
  # Pre-compute the Firehose stream name for dependency resolution
  firehose_stream_name = "${local.name_prefix}-shared-to-iceberg"
  
  common_tags = {
    Environment = var.environment
    Project     = local.project_name
    ManagedBy   = "terraform"
  }
}

# =============================================================================
# MODULE: STORAGE (Foundation Layer)
# =============================================================================
# References existing S3 buckets for Iceberg data and schemas

module "storage" {
  source = "./modules/storage"

  environment         = var.environment
  iceberg_bucket_name = var.iceberg_bucket
  schema_bucket_name  = var.schema_bucket
  tags                = local.common_tags
}

# =============================================================================
# MODULE: STATE (Independent - DynamoDB Tables)
# =============================================================================
# Can be deployed independently

module "state" {
  source = "./modules/state"

  environment = var.environment
  tags        = local.common_tags
}

# =============================================================================
# MODULE: CATALOG (Depends on Storage)
# =============================================================================
# Glue database + Iceberg tables via Athena DDL

module "catalog" {
  source = "./modules/catalog"

  environment    = var.environment
  iceberg_bucket = var.iceberg_bucket
  topics         = var.topics
  tags           = local.common_tags

  depends_on = [module.storage]
}

# =============================================================================
# MODULE: MESSAGING (Independent)
# =============================================================================
# SQS queues + DLQ

module "messaging" {
  source = "./modules/messaging"

  environment = var.environment
  topics      = var.topics
  tags        = local.common_tags
}

# =============================================================================
# MODULE: OBSERVABILITY (After Messaging - needs DLQ name)
# =============================================================================
# SNS alerts + CloudWatch alarms

module "observability" {
  source = "./modules/observability"

  environment                 = var.environment
  dlq_name                    = module.messaging.dlq_name
  sqs_processor_function_name = module.compute.sqs_processor_name
  circuit_breaker_lambda_arn  = module.compute.circuit_breaker_arn
  tags                        = local.common_tags

  depends_on = [module.messaging, module.compute]
}

# =============================================================================
# MODULE: COMPUTE (Depends on Messaging, State)
# =============================================================================
# All Lambda functions - uses pre-computed Firehose name

module "compute" {
  source = "./modules/compute"

  environment              = var.environment
  topics                   = var.topics
  firehose_stream_name     = local.firehose_stream_name  # Pre-computed name
  sqs_queue_arns           = module.messaging.queue_arns
  dlq_arn                  = module.messaging.dlq_arn
  error_tracker_table_arn  = module.state.error_tracker_table_arn
  error_tracker_table_name = module.state.error_tracker_table_name
  schema_bucket            = var.schema_bucket
  archive_bucket           = var.iceberg_bucket  # Reuse Iceberg bucket for DLQ archive
  iceberg_bucket           = var.iceberg_bucket
  checkpoint_table_arn     = module.state.checkpoints_table_arn
  tags                     = local.common_tags

  depends_on = [module.messaging, module.state]
}

# =============================================================================
# MODULE: INGESTION (Depends on Catalog and Compute)
# =============================================================================
# Firehose delivery stream

module "ingestion" {
  source = "./modules/ingestion"

  environment            = var.environment
  database_name          = module.catalog.database_name
  iceberg_bucket         = var.iceberg_bucket
  firehose_transform_arn = module.compute.firehose_transform_arn
  tags                   = local.common_tags

  depends_on = [module.catalog, module.compute]
}

# =============================================================================
# MODULE: ORCHESTRATION (Depends on Compute, Observability)
# =============================================================================
# Step Functions + EventBridge + Glue job

module "orchestration" {
  source = "./modules/orchestration"

  environment                     = var.environment
  topics                          = var.topics
  schedule_expression             = var.semantic_schedule
  iceberg_bucket                  = var.iceberg_bucket
  schema_bucket                   = var.iceberg_bucket
  glue_role_arn                   = module.compute.glue_role_arn
  check_schema_lambda_arn         = module.compute.check_schema_exists_arn
  ensure_curated_table_lambda_arn = module.compute.ensure_curated_table_arn
  alerts_topic_arn                = module.observability.alerts_topic_arn
  tags                            = local.common_tags

  depends_on = [module.compute, module.observability, module.ingestion]
}

# =============================================================================
# SQS LAMBDA TRIGGERS (Connect Messaging to Compute)
# =============================================================================
# These connect the messaging module to the compute module

resource "aws_lambda_event_source_mapping" "sqs_triggers" {
  for_each = toset(var.topics)

  event_source_arn = module.messaging.queue_arns[each.key]
  function_name    = module.compute.sqs_processor_arn
  batch_size       = 10
  enabled          = true

  # FAIL-AWARE PATTERNS (Wave 1)
  # Enable partial batch failure reporting - without this, batchItemFailures is ignored
  function_response_types = ["ReportBatchItemFailures"]
  
  # Limit concurrent Lambda invocations to protect downstream systems
  scaling_config {
    maximum_concurrency = var.maximum_concurrency
  }

  depends_on = [module.compute, module.messaging, module.ingestion]
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "sqs_queue_urls" {
  description = "SQS queue URLs by topic"
  value       = module.messaging.queue_urls
}

output "centralized_dlq_url" {
  description = "Centralized DLQ URL"
  value       = module.messaging.dlq_url
}

output "firehose_stream_name" {
  description = "Firehose stream name"
  value       = module.ingestion.stream_name
}

output "dynamodb_tables" {
  description = "DynamoDB table names"
  value       = module.state.all_table_names
}

output "state_machine_arn" {
  description = "Step Functions state machine ARN"
  value       = module.orchestration.state_machine_arn
}

output "alerts_topic_arn" {
  description = "SNS alerts topic ARN"
  value       = module.observability.alerts_topic_arn
}
