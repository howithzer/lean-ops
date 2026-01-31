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

# Add partitioning to RAW staging tables (must be done after table creation)
resource "null_resource" "partition_topic_tables" {
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
      
      echo "Adding partition field to ${each.key}_staging..."
      
      # Partition by day(publish_time) for accountability alignment with GCP Pub/Sub
      ALTER_SQL="ALTER TABLE ${var.database_name}.${each.key}_staging 
        ADD IF NOT EXISTS PARTITION FIELD day(to_timestamp(publish_time))"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$ALTER_SQL" \
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
          echo "Partition added to ${each.key}_staging successfully!"
          exit 0
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          echo "Query failed: $ERROR"
          # Don't fail if partition already exists
          if echo "$ERROR" | grep -q "Partition already exists"; then
            echo "Partition already exists, continuing..."
            exit 0
          fi
          exit 1
        fi
        
        sleep 5
      done
      
      echo "Timeout waiting for Athena query"
      exit 1
    EOT
  }

  depends_on = [null_resource.create_topic_tables]
}

# =============================================================================
# STANDARDIZED LAYER - Database and Table
# =============================================================================

resource "aws_glue_catalog_database" "standardized" {
  name        = "iceberg_standardized_db"
  description = "Standardized layer database for ${var.project_name} - flattened, deduplicated, all STRING types"
}

# Standardized events table with composite partitioning
resource "null_resource" "create_standardized_table" {
  triggers = {
    database = aws_glue_catalog_database.standardized.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Standardized Iceberg table via Athena DDL..."
      
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
          echo "Standardized events table created successfully!"
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

  depends_on = [aws_glue_catalog_database.standardized]
}

# Add composite partitioning to Standardized table
resource "null_resource" "partition_standardized_table" {
  triggers = {
    database = aws_glue_catalog_database.standardized.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Adding composite partitioning to Standardized table..."
      
      # Partition 1: period_reference (for period-end reconciliation)
      ALTER_SQL_1="ALTER TABLE iceberg_standardized_db.events 
        ADD IF NOT EXISTS PARTITION FIELD period_reference"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$ALTER_SQL_1" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Adding period_reference partition... Query ID: $QUERY_ID"
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "period_reference partition added!"
          break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          if echo "$ERROR" | grep -q "Partition already exists"; then
            echo "period_reference partition already exists"
            break
          fi
          echo "ERROR: $ERROR"
          exit 1
        fi
        
        sleep 5
      done
      
      # Partition 2: day(publish_time) (for incremental processing)
      ALTER_SQL_2="ALTER TABLE iceberg_standardized_db.events 
        ADD IF NOT EXISTS PARTITION FIELD day(to_timestamp(publish_time))"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$ALTER_SQL_2" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Adding day(publish_time) partition... Query ID: $QUERY_ID"
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "day(publish_time) partition added!"
          echo "Standardized table partitioning complete!"
          exit 0
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          if echo "$ERROR" | grep -q "Partition already exists"; then
            echo "day(publish_time) partition already exists"
            exit 0
          fi
          echo "ERROR: $ERROR"
          exit 1
        fi
        
        sleep 5
      done
      
      echo "Timeout waiting for Athena query"
      exit 1
    EOT
  }

  depends_on = [null_resource.create_standardized_table]
}

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

  depends_on = [aws_glue_catalog_database.standardized]
}

# =============================================================================
# CURATED LAYER - Database and Tables
# =============================================================================

resource "aws_glue_catalog_database" "curated" {
  name        = "iceberg_curated_db"
  description = "Curated layer database for ${var.project_name} - typed, governed, validated"
}

# Curated events table (typed columns)
resource "null_resource" "create_curated_events" {
  triggers = {
    database = aws_glue_catalog_database.curated.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Curated events table via Athena DDL..."
      
      DDL="CREATE TABLE IF NOT EXISTS iceberg_curated_db.events (
        message_id          STRING,
        idempotency_key     STRING,
        period_reference    STRING,
        
        application_id      INT,
        event_type          STRING,
        verb                STRING,
        session_id          STRING,
        user_id             STRING,
        
        amount              DECIMAL(10,2),
        sensor_reading      DOUBLE,
        
        event_timestamp     TIMESTAMP,
        publish_time        TIMESTAMP,
        ingestion_ts        BIGINT,
        
        first_seen_ts       TIMESTAMP,
        last_updated_ts     TIMESTAMP,
        _schema_version     STRING
      )
      LOCATION 's3://${var.iceberg_bucket}/iceberg_curated_db/events/'
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
          echo "Curated events table created successfully!"
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

  depends_on = [aws_glue_catalog_database.curated]
}

# Add composite partitioning to Curated events table
resource "null_resource" "partition_curated_events" {
  triggers = {
    database = aws_glue_catalog_database.curated.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Adding composite partitioning to Curated events table..."
      
      # Partition 1: period_reference (for period-end reconciliation)
      ALTER_SQL_1="ALTER TABLE iceberg_curated_db.events 
        ADD IF NOT EXISTS PARTITION FIELD period_reference"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$ALTER_SQL_1" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Adding period_reference partition... Query ID: $QUERY_ID"
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "period_reference partition added!"
          break
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          if echo "$ERROR" | grep -q "Partition already exists"; then
            echo "period_reference partition already exists"
            break
          fi
          echo "ERROR: $ERROR"
          exit 1
        fi
        
        sleep 5
      done
      
      # Partition 2: day(publish_time) (for incremental processing)
      # Note: Curated has TIMESTAMP type, so no to_timestamp() needed
      ALTER_SQL_2="ALTER TABLE iceberg_curated_db.events 
        ADD IF NOT EXISTS PARTITION FIELD day(publish_time)"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$ALTER_SQL_2" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Adding day(publish_time) partition... Query ID: $QUERY_ID"
      
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "day(publish_time) partition added!"
          echo "Curated events table partitioning complete!"
          exit 0
        elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "CANCELLED" ]; then
          ERROR=$(aws athena get-query-execution \
            --query-execution-id "$QUERY_ID" \
            --query "QueryExecution.Status.StateChangeReason" \
            --output text)
          if echo "$ERROR" | grep -q "Partition already exists"; then
            echo "day(publish_time) partition already exists"
            exit 0
          fi
          echo "ERROR: $ERROR"
          exit 1
        fi
        
        sleep 5
      done
      
      echo "Timeout waiting for Athena query"
      exit 1
    EOT
  }

  depends_on = [null_resource.create_curated_events]
}

# Curated errors table (CDE violations, type failures)
resource "null_resource" "create_curated_errors" {
  triggers = {
    database = aws_glue_catalog_database.curated.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Curated errors table via Athena DDL..."
      
      DDL="CREATE TABLE IF NOT EXISTS iceberg_curated_db.errors (
        message_id        STRING,
        idempotency_key   STRING,
        raw_record        STRING COMMENT 'Serialized source record',
        error_type        STRING COMMENT 'CDE_VIOLATION, TYPE_CAST_ERROR, ENUM_INVALID',
        error_field       STRING COMMENT 'Which field failed validation',
        error_message     STRING,
        processed_ts      TIMESTAMP
      )
      LOCATION 's3://${var.iceberg_bucket}/iceberg_curated_db/errors/'
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
          echo "Curated errors table created successfully!"
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

  depends_on = [aws_glue_catalog_database.curated]
}

# Curated drift_log table (schema changes tracking)
resource "null_resource" "create_curated_drift_log" {
  triggers = {
    database = aws_glue_catalog_database.curated.name
    bucket   = var.iceberg_bucket
  }

  provisioner "local-exec" {
    interpreter = ["bash", "-c"]
    command     = <<-EOT
      set -e
      
      echo "Creating Curated drift_log table via Athena DDL..."
      
      DDL="CREATE TABLE IF NOT EXISTS iceberg_curated_db.drift_log (
        detected_ts       TIMESTAMP,
        column_name       STRING,
        action            STRING COMMENT 'ADDED, REMOVED, TYPE_CHANGED',
        source_layer      STRING COMMENT 'standardized, curated',
        old_value         STRING,
        new_value         STRING,
        details           STRING
      )
      LOCATION 's3://${var.iceberg_bucket}/iceberg_curated_db/drift_log/'
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
          echo "drift_log table created successfully!"
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

output "standardized_table_created" {
  description = "Indicates Standardized events table was created"
  value       = null_resource.create_standardized_table.id
}

output "curated_tables_created" {
  description = "Indicates Curated tables were created"
  value = {
    events    = null_resource.create_curated_events.id
    errors    = null_resource.create_curated_errors.id
    drift_log = null_resource.create_curated_drift_log.id
  }
}
