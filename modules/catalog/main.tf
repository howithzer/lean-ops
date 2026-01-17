# =============================================================================
# CATALOG MODULE - Glue Database and Iceberg Tables
# =============================================================================
# This module creates:
# - Glue catalog database for RAW layer
# - Iceberg tables via Athena DDL (required before Firehose)
# =============================================================================

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
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
  description = "Glue database name for RAW layer"
  type        = string
  default     = "iceberg_raw_db"
}

variable "iceberg_bucket" {
  description = "S3 bucket for Iceberg data"
  type        = string
}

variable "topics" {
  description = "List of topics to create staging tables for"
  type        = list(string)
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
  common_tags = merge(var.tags, {
    Module = "catalog"
  })
}

# =============================================================================
# GLUE DATABASE
# =============================================================================

resource "aws_glue_catalog_database" "raw" {
  name        = var.database_name
  description = "RAW layer database for ${var.project_name}"
}

# =============================================================================
# ICEBERG TABLES VIA ATHENA DDL
# =============================================================================

# Default staging table (required for Firehose destination)
resource "null_resource" "create_default_table" {
  triggers = {
    database = var.database_name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Iceberg table via Athena DDL..."
      
      DDL="CREATE TABLE IF NOT EXISTS ${var.database_name}.default_staging (
        message_id        STRING,
        topic_name        STRING,
        json_payload      STRING,
        ingestion_ts      BIGINT
      )
      LOCATION 's3://${var.iceberg_bucket}/${var.database_name}/default_staging/'
      TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet')"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Athena Query ID: $QUERY_ID"
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        echo "Query status: $STATUS"
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "Iceberg table created successfully!"
          exit 0
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          echo "Query failed: $ERROR"
          exit 1
        fi
        
        sleep 5
      done
      
      echo "Timeout waiting for Athena query"
      exit 1
    EOT
  }

  depends_on = [aws_glue_catalog_database.raw]
}

# Per-topic staging tables
resource "null_resource" "create_topic_tables" {
  for_each = toset(var.topics)

  triggers = {
    topic    = each.key
    database = var.database_name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Iceberg table ${each.key}_staging via Athena DDL..."
      
      DDL="CREATE TABLE IF NOT EXISTS ${var.database_name}.${each.key}_staging (
        topic_name        STRING,
        message_id        STRING,
        idempotency_key   STRING,
        period_reference  STRING,
        correlation_id    STRING,
        publish_time      STRING,
        ingestion_ts      BIGINT,
        json_payload      STRING
      )
      LOCATION 's3://${var.iceberg_bucket}/${var.database_name}/${each.key}_staging/'
      TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet')"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Athena Query ID: $QUERY_ID"
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        echo "Query status: $STATUS"
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "Table ${each.key}_staging created successfully!"
          exit 0
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          echo "Query failed: $ERROR"
          exit 1
        fi
        
        sleep 5
      done
      
      echo "Timeout waiting for Athena query"
      exit 1
    EOT
  }

  depends_on = [aws_glue_catalog_database.raw]
}

# =============================================================================
# CURATED LAYER - Database and Table
# =============================================================================

resource "aws_glue_catalog_database" "standardized" {
  name        = "iceberg_standardized_db"
  description = "Curated layer database for ${var.project_name} - flattened, deduplicated, all STRING types"
}

# Curated events table with composite partitioning
resource "null_resource" "create_standardized_table" {
  triggers = {
    database = aws_glue_catalog_database.curated.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Curated Iceberg table via Athena DDL..."
      
      DDL="CREATE TABLE IF NOT EXISTS iceberg_standardized_db.events (
        message_id        STRING,
        idempotency_key   STRING,
        period_reference  STRING,
        correlation_id    STRING,
        publish_time      STRING,
        ingestion_ts      BIGINT,
        topic_name        STRING,
        device_id         STRING,
        event_type        STRING,
        event_timestamp   STRING,
        sensor_reading    STRING,
        location          STRING,
        status            STRING,
        amount            STRING,
        currency          STRING,
        user_id           STRING
      )
      LOCATION 's3://${var.iceberg_bucket}/iceberg_standardized_db/events/'
      TBLPROPERTIES ('table_type' = 'ICEBERG', 'format' = 'parquet')"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Athena Query ID: $QUERY_ID"
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        echo "Query status: $STATUS"
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "Curated events table created successfully!"
          exit 0
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          echo "Query failed: $ERROR"
          exit 1
        fi
        
        sleep 5
      done
      
      echo "Timeout waiting for Athena query"
      exit 1
    EOT
  }

  depends_on = [aws_glue_catalog_database.curated]
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "database_name" {
  description = "Glue database name"
  value       = aws_glue_catalog_database.raw.name
}

output "standardized_database_name" {
  description = "Curated Glue database name"
  value       = aws_glue_catalog_database.curated.name
}

output "default_table_created" {
  description = "Indicates default_staging table was created"
  value       = null_resource.create_default_table.id
}

output "topic_tables_created" {
  description = "Map of topic table creation IDs"
  value       = { for k, v in null_resource.create_topic_tables : k => v.id }
}

output "curated_table_created" {
  description = "Indicates Curated events table was created"
  value       = null_resource.create_standardized_table.id
}

