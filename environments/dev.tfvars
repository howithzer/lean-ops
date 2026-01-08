# Lean-Ops: Dev Environment

aws_region  = "us-east-1"
environment = "dev"

# Topics to deploy
topics = ["events", "orders", "payments"]

# S3 buckets
iceberg_bucket = "lean-ops-dev-iceberg"
schema_bucket  = "lean-ops-dev-schemas"

# Glue role (must exist)
glue_role_arn = "arn:aws:iam::YOUR_ACCOUNT_ID:role/GlueServiceRole"

# Scaling
maximum_concurrency = 50

# Scheduling
semantic_schedule = "rate(15 minutes)"
