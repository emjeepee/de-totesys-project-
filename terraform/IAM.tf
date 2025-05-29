



# Create a permission for the second lambda so that it can
# receive events from the EventBridge to allow the invocation
# of the second lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.xxx-second-lambda-function-xxx
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.invoke_second_lambda.arn
                                                     }
