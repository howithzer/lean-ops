data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_orchestrator_role" {
  name               = "${var.environment}-sfn-orchestrator-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
}

data "aws_iam_policy_document" "sfn_orchestrator_policy" {
  # Allow Orchestrator to List its own executions to prevent overlapping runs
  statement {
    actions = ["states:ListExecutions"]
    resources = ["arn:aws:states:*:*:stateMachine:${var.environment}-pipeline-orchestrator"]
  }

  # Allow Orchestrator to invoke the Fetch Topics Lambda
  statement {
    actions = ["lambda:InvokeFunction"]
    resources = [var.fetch_topics_lambda_arn]
  }

  # Allow Orchestrator to trigger Glue Jobs (Sync)
  statement {
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun"
    ]
    resources = ["*"]
  }

  # Allow Historical Rebuilder to pause/resume pipeline in DynamoDB Registry
  statement {
    actions = ["dynamodb:UpdateItem"]
    resources = [
      "arn:aws:dynamodb:*:*:table/${var.pipeline_registry_table}"
    ]
  }
}

resource "aws_iam_policy" "sfn_orchestrator_policy" {
  name        = "${var.environment}-sfn-orchestrator-policy"
  description = "Policy for Master Orchestrator Step Function"
  policy      = data.aws_iam_policy_document.sfn_orchestrator_policy.json
}

resource "aws_iam_role_policy_attachment" "sfn_orchestrator_attach" {
  role       = aws_iam_role.sfn_orchestrator_role.name
  policy_arn = aws_iam_policy.sfn_orchestrator_policy.arn
}
