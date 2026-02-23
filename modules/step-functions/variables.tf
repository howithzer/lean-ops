variable "environment" {
  description = "Environment name"
  type        = string
}

variable "fetch_topics_lambda_arn" {
  description = "ARN of the Lambda that fetches active topics"
  type        = string
}

variable "standardized_job_name" {
  description = "Name of the Standardized Glue Job"
  type        = string
}

variable "curated_job_name" {
  description = "Name of the Curated Glue Job"
  type        = string
}

variable "historical_rebuilder_job_name" {
  description = "Name of the Historical Rebuilder Glue Job"
  type        = string
}

variable "pipeline_registry_table" {
  description = "Name of the DynamoDB Pipeline Registry table"
  type        = string
}

variable "raw_database" {
  description = "Name of the RAW catalog database"
  type        = string
}

variable "standardized_database" {
  description = "Name of the STANDARDIZED catalog database"
  type        = string
}

variable "curated_database" {
  description = "Name of the CURATED catalog database"
  type        = string
}

variable "checkpoint_table" {
  description = "Name of the DynamoDB Checkpoints table"
  type        = string
}

variable "iceberg_bucket" {
  description = "Name of the S3 bucket storing Iceberg data"
  type        = string
}

variable "schema_bucket" {
  description = "Name of the S3 bucket storing schemas"
  type        = string
}

variable "tags" {
  description = "A map of tags"
  type        = map(string)
  default     = {}
}
