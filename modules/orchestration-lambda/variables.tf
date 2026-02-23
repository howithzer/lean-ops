variable "environment" {
  description = "Environment name"
  type        = string
}

variable "pipeline_registry_arn" {
  description = "ARN of the Pipeline Registry DynamoDB table"
  type        = string
}

variable "pipeline_registry_name" {
  description = "Name of the Pipeline Registry DynamoDB table"
  type        = string
}

variable "tags" {
  description = "Tags map"
  type        = map(string)
  default     = {}
}
