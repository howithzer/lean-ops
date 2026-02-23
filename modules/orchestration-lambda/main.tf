data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_execution_role" {
  name               = "${var.environment}-fetch-topics-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_dynamodb_policy" {
  statement {
    actions = [
      "dynamodb:Scan",
      "dynamodb:Query",
      "dynamodb:GetItem"
    ]
    resources = [var.pipeline_registry_arn]
  }
}

resource "aws_iam_policy" "lambda_dynamodb_policy" {
  name        = "${var.environment}-fetch-topics-dynamodb-policy"
  description = "Allow Lambda to read from DynamoDB pipeline registry"
  policy      = data.aws_iam_policy_document.lambda_dynamodb_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_dynamodb_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_dynamodb_policy.arn
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/fetch_active_topics.py"
  output_path = "${path.module}/fetch_active_topics.zip"
}

resource "aws_lambda_function" "fetch_active_topics" {
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  function_name    = "${var.environment}-fetch-active-topics"
  role             = aws_iam_role.lambda_execution_role.arn
  handler          = "fetch_active_topics.lambda_handler"
  runtime          = "python3.12"
  timeout          = 30

  environment {
    variables = {
      PIPELINE_REGISTRY_TABLE = var.pipeline_registry_name
    }
  }

  tags = var.tags
}
