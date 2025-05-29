resource "aws_sns_topic" "lambda_alerts" {
  name = "lambda-fatal-error-alerts"
}

resource "aws_sns_topic_subscription" "email_alerts"{
    topic_arn = aws_sns_topic.lambda_alerts.arn
    protocol = "email"
    endpoint = var.alert_email_address
}

resource "aws_cloudwatch_metric_alarm" "fatal_error"{
    alarm_name = "LambdaFatalErrors"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = "1"
    metric_name = "Errors"
    statistic = "Sum"
    threshold = 1
    alarm_description = "Alert If Lambda function errors"
    alarm_actions = [aws_sns_topic.lambda_alerts.arn]
    dimensions = {
      FunctionName = var.first_lambda_function
    }

}