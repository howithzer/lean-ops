resource "aws_dynamodb_table" "pipeline_registry" {
  name         = "${var.environment}-pipeline-registry"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "topic_name"

  attribute {
    name = "topic_name"
    type = "S"
  }

  tags = var.tags
}
