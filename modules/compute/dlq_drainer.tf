
# =============================================================================
# DLQ DRAINER LAMBDA
# =============================================================================

data "archive_file" "dlq_drainer" {
  type        = "zip"
  source_file = "${path.module}/../../scripts/lambda/dlq_drainer.py"
  output_path = "${path.module}/build/dlq_drainer.zip"
}

resource "aws_iam_role" "dlq_drainer" {
  name = "${local.name_prefix}-dlq-drainer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "dlq_drainer" {
  name = "${local.name_prefix}-dlq-drainer-policy"
  role = aws_iam_role.dlq_drainer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:DeleteMessageBatch",
          "sqs:GetQueueAttributes"
        ]
        Resource = var.dlq_arn
      },
      {
        Effect = "Allow"
        Action = ["s3:PutObject"]
        # Allow writing to dlq_errors/ prefix in archive bucket
        Resource = "arn:aws:s3:::${var.archive_bucket}/dlq_errors/*"
      },
      {
        Effect = "Allow"
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_lambda_function" "dlq_drainer" {
  filename         = data.archive_file.dlq_drainer.output_path
  function_name    = "${local.name_prefix}-dlq-drainer"
  role             = aws_iam_role.dlq_drainer.arn
  handler          = "dlq_drainer.lambda_handler"
  source_code_hash = data.archive_file.dlq_drainer.output_base64sha256
  runtime          = "python3.11"
  timeout          = 900  # 15 minutes (Max for Lambda)
  memory_size      = 256
  reserved_concurrent_executions = 1  # Safety: Single instance only

  environment {
    variables = {
      DLQ_URL       = var.dlq_url
      TARGET_BUCKET = var.archive_bucket
      TARGET_PREFIX = "dlq_errors"
    }
  }

  tags = local.common_tags
}

# Schedule Trigger (Hourly)
resource "aws_cloudwatch_event_rule" "dlq_drainer_schedule" {
  name                = "${local.name_prefix}-dlq-drainer-schedule"
  description         = "Triggers DLQ Drainer every hour"
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "dlq_drainer_target" {
  rule      = aws_cloudwatch_event_rule.dlq_drainer_schedule.name
  target_id = "TriggerDLQDrainer"
  arn       = aws_lambda_function.dlq_drainer.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_dlq_drainer" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dlq_drainer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.dlq_drainer_schedule.arn
}
