output "table_name" {
  value = aws_dynamodb_table.pipeline_registry.name
}

output "table_arn" {
  value = aws_dynamodb_table.pipeline_registry.arn
}
