
# =============================================================================
# DLQ ERRORS TABLE (JSON)
# =============================================================================

resource "null_resource" "create_dlq_errors_table" {
  triggers = {
    always_run = timestamp()
  }

  provisioner "local-exec" {
    command = <<EOT
      echo "Creating DLQ Errors table via Athena DDL..."
      
      DDL="CREATE EXTERNAL TABLE IF NOT EXISTS ${var.database_name}.dlq_errors (
        message_id          STRING,
        receipt_handle      STRING,
        body                STRING,
        attributes          MAP<STRING, STRING>,
        message_attributes  MAP<STRING, struct<StringValue:STRING, BinaryValue:STRING, DataType:STRING>>,
        error_cause         STRING,
        drained_at          STRING
      )
      ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
      LOCATION 's3://${var.iceberg_bucket}/dlq_errors/'"
      
      QUERY_ID=$(aws athena start-query-execution \
        --query-string "$DDL" \
        --work-group "primary" \
        --result-configuration "OutputLocation=s3://${var.iceberg_bucket}/athena-results/" \
        --query "QueryExecutionId" \
        --output text)
      
      echo "Athena Query ID: $QUERY_ID"
      
      # Wait for query completion
      for i in {1..12}; do
        STATUS=$(aws athena get-query-execution \
          --query-execution-id "$QUERY_ID" \
          --query "QueryExecution.Status.State" \
          --output text)
        
        echo "Query status: $STATUS"
        
        if [ "$STATUS" = "SUCCEEDED" ]; then
          echo "DLQ Errors table created successfully!"
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
