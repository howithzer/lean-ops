resource "aws_sfn_state_machine" "pipeline_orchestrator" {
  name     = "${var.environment}-pipeline-orchestrator"
  role_arn = aws_iam_role.sfn_orchestrator_role.arn

  definition = templatefile("${path.module}/definitions/orchestrator.asl.json", {
    fetch_topics_lambda_arn = var.fetch_topics_lambda_arn
    standardized_job_name   = var.standardized_job_name
    curated_job_name        = var.curated_job_name
    raw_database            = var.raw_database
    standardized_database   = var.standardized_database
    curated_database        = var.curated_database
    checkpoint_table        = var.checkpoint_table
    iceberg_bucket          = var.iceberg_bucket
    schema_bucket           = var.schema_bucket
    state_machine_arn       = "arn:aws:states:$${AWS::Region}:$${AWS::AccountId}:stateMachine:${var.environment}-pipeline-orchestrator"
  })

  tags = var.tags
}

resource "aws_sfn_state_machine" "historical_rebuilder" {
  name     = "${var.environment}-historical-rebuilder"
  role_arn = aws_iam_role.sfn_orchestrator_role.arn

  definition = templatefile("${path.module}/definitions/rebuilder.asl.json", {
    rebuilder_job_name      = var.historical_rebuilder_job_name
    pipeline_registry_table = var.pipeline_registry_table
  })

  tags = var.tags
}
