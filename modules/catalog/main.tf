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

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e

      echo "Dropping Iceberg table default_staging..."

      DDL="DROP TABLE IF EXISTS ${self.triggers.database}.default_staging"

      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${self.triggers.bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text 2>/dev/null || echo "")

      if [ -n "$QUERY_ID" ] && [ "$QUERY_ID" != "None" ]; then
        echo "Athena Query ID: $QUERY_ID"

        for i in {1..12}; do
          STATUS=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.State" \
            --output text 2>/dev/null || echo "UNKNOWN")

          echo "Query status: $STATUS"

          if [ "$STATUS" = "SUCCEEDED" ]; then
            echo "Table default_staging dropped successfully!"
            exit 0
          elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
            echo "Drop table query failed (table may not exist)"
            exit 0
          fi

          sleep 5
        done
      else
        echo "No query ID returned, table may not exist"
      fi

      exit 0
    EOT
  }

  depends_on = [aws_glue_catalog_database.raw]
}

# Per-topic staging tables
resource "null_resource" "create_topic_tables" {
  for_each = toset(var.topics)

  triggers = {
    topic       = each.key
    database    = var.database_name
    bucket      = var.iceberg_bucket
    schema_hash = md5("topic_name,message_id,idempotency_key,period_reference,correlation_id,publish_time,ingestion_ts,json_payload")
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
        publish_time      TIMESTAMP,
        ingestion_ts      BIGINT,
        json_payload      STRING
      )
      PARTITIONED BY (day(publish_time))
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

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e

      echo "Dropping Iceberg table ${self.triggers.topic}_staging..."

      DDL="DROP TABLE IF EXISTS ${self.triggers.database}.${self.triggers.topic}_staging"

      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${self.triggers.bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text 2>/dev/null || echo "")

      if [ -n "$QUERY_ID" ] && [ "$QUERY_ID" != "None" ]; then
        echo "Athena Query ID: $QUERY_ID"

        for i in {1..12}; do
          STATUS=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.State" \
            --output text 2>/dev/null || echo "UNKNOWN")

          echo "Query status: $STATUS"

          if [ "$STATUS" = "SUCCEEDED" ]; then
            echo "Table ${self.triggers.topic}_staging dropped successfully!"
            exit 0
          elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
            echo "Drop table query failed (table may not exist)"
            exit 0
          fi

          sleep 5
        done
      else
        echo "No query ID returned, table may not exist"
      fi

      exit 0
    EOT
  }

  depends_on = [aws_glue_catalog_database.raw]
}


# NOTE: Partitioning is defined in CREATE TABLE using PARTITIONED BY clause
# Athena supports partitioning AT TABLE CREATION, but not via ALTER TABLE afterwards

# =============================================================================
# STANDARDIZED LAYER - Database and Table
# =============================================================================

resource "aws_glue_catalog_database" "standardized" {
  name        = "iceberg_standardized_db"
  description = "Standardized layer database for ${var.project_name} - flattened, deduplicated, all STRING types"
}

# ==========================================================================================
# STANDARDIZED TABLES - Created dynamically by Glue job from schema file
# ==========================================================================================
# NOTE: Standardized tables are NOT created by Terraform.
# They are created by the standardized_processor Glue job when schema file is deployed.
# This enables schema-driven deployment:
#   - Day 1: Terraform creates databases only
#   - Day N: Schema deployed → Glue creates iceberg_standardized_db.{topic} table
#
# Table naming: iceberg_standardized_db.{topic_name} (e.g., "events", "orders", "payments")
# Partitioning: Added by Glue job using Spark SQL (Athena doesn't support this for Iceberg)
# ==========================================================================================

# Standardized parse_errors table for error handling
resource "null_resource" "create_standardized_parse_errors" {
  triggers = {
    database = aws_glue_catalog_database.standardized.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Standardized parse_errors table via Athena DDL..."
      
      DDL="CREATE TABLE IF NOT EXISTS iceberg_standardized_db.parse_errors (
        message_id        STRING,
        idempotency_key   STRING,
        raw_payload       STRING COMMENT 'Original json_payload for debugging',
        error_type        STRING COMMENT 'PARSE_ERROR, FLATTEN_ERROR',
        error_message     STRING,
        ingestion_ts      BIGINT,
        processed_ts      TIMESTAMP
      )
      LOCATION 's3://${var.iceberg_bucket}/iceberg_standardized_db/parse_errors/'
      TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet'
      )"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "parse_errors table created successfully!"
          exit 0
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          echo "Query failed"
          exit 1
        fi
        sleep 5
      done
      exit 1
    EOT
  }

  provisioner "local-exec" {
    when        = destroy
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e

      echo "Dropping Standardized parse_errors table..."

      DDL="DROP TABLE IF EXISTS ${self.triggers.database}.parse_errors"

      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${self.triggers.bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text 2>/dev/null || echo "")

      if [ -n "$QUERY_ID" ] && [ "$QUERY_ID" != "None" ]; then
        echo "Athena Query ID: $QUERY_ID"

        for i in {1..12}; do
          STATUS=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.State" \
            --output text 2>/dev/null || echo "UNKNOWN")

          echo "Query status: $STATUS"

          if [ "$STATUS" = "SUCCEEDED" ]; then
            echo "Table parse_errors dropped successfully!"
            exit 0
          elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
            echo "Drop table query failed (table may not exist)"
            exit 0
          fi

          sleep 5
        done
      else
        echo "No query ID returned, table may not exist"
      fi

      exit 0
    EOT
  }

  depends_on = [aws_glue_catalog_database.standardized]
}

# =============================================================================
# CURATED LAYER - Database and Tables
# =============================================================================

resource "aws_glue_catalog_database" "curated" {
  name        = "iceberg_curated_db"
  description = "Curated layer database for ${var.project_name} - typed, governed, validated"
}


# ==========================================================================================
# CURATED TABLES - Created dynamically by Glue job from schema file
# ==========================================================================================
# NOTE: Curated tables (events, errors, drift_log) are NOT created by Terraform.
# They are created by the curated_processor Glue job when the schema file is deployed.
# This enables schema-driven deployment:
#   - Day 1: Terraform creates RAW/Standardized tables (topic schema not needed yet)
#   - Day N: Schema file deployed to S3 → Glue job creates Curated table from schema
#
# This decoupling allows RAW ingestion to start immediately while business teams
# finalize the typed schema for Curated layer.
# ==========================================================================================

# =============================================================================
# OUTPUTS
# =============================================================================

output "database_name" {
  description = "Glue database name"
  value       = aws_glue_catalog_database.raw.name
}

output "standardized_database_name" {
  description = "Standardized Glue database name"
  value       = aws_glue_catalog_database.standardized.name
}

output "curated_database_name" {
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

