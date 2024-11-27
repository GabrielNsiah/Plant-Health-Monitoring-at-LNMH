provider "aws" {
  region = "eu-west-2"
}

variable "lambda_function_arn_minute" {
  default = "arn:aws:lambda:eu-west-2:129033205317:function:c14-gbu-lambda"
}

variable "lambda_function_arn_midnight" {
  default = "arn:aws:lambda:eu-west-2:129033205317:function:c14-gbu-lambda-midnight"
}

resource "aws_iam_role" "eventbridge_execution_role" {
  name = "c14-gbu-event-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "events.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "eventbridge_execution_role_attach" {
  name       = "eventbridge-lambda-execution"
  roles      = [aws_iam_role.eventbridge_execution_role.name]
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaRole"
}

resource "aws_cloudwatch_event_rule" "c14_gbu_minute_rule" {
  name                = "c14-gbu-minute"
  description         = "Trigger Lambda every minute"
  schedule_expression = "cron(* * * * ? *)"
}

resource "aws_cloudwatch_event_target" "c14_gbu_minute_target" {
  rule = aws_cloudwatch_event_rule.c14_gbu_minute_rule.name
  arn  = var.lambda_function_arn_minute

  retry_policy {
    maximum_retry_attempts     = 185
    maximum_event_age_in_seconds = 86400
  }
}

resource "aws_lambda_permission" "allow_eventbridge_minute" {
  statement_id  = "AllowEventBridgeInvokeMinute"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_function_arn_minute
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.c14_gbu_minute_rule.arn
}

resource "aws_cloudwatch_event_rule" "c14_gbu_midnight_rule" {
  name                = "c14-gbu-midnight"
  description         = "Trigger Lambda every midnight"
  schedule_expression = "cron(0 0 * * ? *)"
}

resource "aws_cloudwatch_event_target" "c14_gbu_midnight_target" {
  rule = aws_cloudwatch_event_rule.c14_gbu_midnight_rule.name
  arn  = var.lambda_function_arn_midnight

  retry_policy {
    maximum_retry_attempts     = 185
    maximum_event_age_in_seconds = 86400
  }
}

resource "aws_lambda_permission" "allow_eventbridge_midnight" {
  statement_id  = "AllowEventBridgeInvokeMidnight"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_function_arn_midnight
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.c14_gbu_midnight_rule.arn
}

