# =============================================================================
# STORAGE MODULE - S3 Buckets for Iceberg Data Lake
# =============================================================================
# This module creates and configures S3 buckets for:
# - Iceberg table data storage
# - Athena query results
# - Schema files
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
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "lean-ops"
}

variable "iceberg_bucket_name" {
  description = "Name of the S3 bucket for Iceberg data (must exist)"
  type        = string
}

variable "schema_bucket_name" {
  description = "Name of the S3 bucket for schema files (must exist)"
  type        = string
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
  common_tags = merge(var.tags, {
    Module = "storage"
  })
}

# =============================================================================
# DATA SOURCES - Reference existing buckets
# =============================================================================

# Iceberg data bucket (created manually, referenced here)
data "aws_s3_bucket" "iceberg" {
  bucket = var.iceberg_bucket_name
}

# Schema bucket (created manually, referenced here)
data "aws_s3_bucket" "schemas" {
  bucket = var.schema_bucket_name
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "iceberg_bucket_name" {
  description = "Iceberg data bucket name"
  value       = data.aws_s3_bucket.iceberg.id
}

output "iceberg_bucket_arn" {
  description = "Iceberg data bucket ARN"
  value       = data.aws_s3_bucket.iceberg.arn
}

output "iceberg_bucket_region" {
  description = "Iceberg data bucket region"
  value       = data.aws_s3_bucket.iceberg.region
}

output "schema_bucket_name" {
  description = "Schema bucket name"
  value       = data.aws_s3_bucket.schemas.id
}

output "schema_bucket_arn" {
  description = "Schema bucket ARN"
  value       = data.aws_s3_bucket.schemas.arn
}
