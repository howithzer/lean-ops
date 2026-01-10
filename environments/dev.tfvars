# Lean-Ops: Dev Environment

aws_region  = "us-east-1"
environment = "dev"

# Topics to deploy
topics = ["events", "orders", "payments"]

# S3 buckets
iceberg_bucket = "lean-ops-development-iceberg"
schema_bucket  = "lean-ops-development-schemas"

# Glue role (will be created by Terraform)
glue_role_arn = "arn:aws:iam::487500748616:role/lean-ops-dev-glue-role"

# Scaling
maximum_concurrency = 50

# Scheduling
semantic_schedule = "rate(15 minutes)"
