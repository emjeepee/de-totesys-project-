resource "aws_cloudwatch_event_bus" "trigger_second_lambda"{
    name = "cloud_trigger_second_lambda"
}


resource "aws_cloudwatch_event_rule" "trigger_on_s3_bucket_change" {
  name        = "capture-ingestion-s3-bucket-change"
  description = "Capture changes in s3 ingestion bucket files"
  event_bus_name = aws_cloudwatch_event_bus.trigger_second_lambda

  event_pattern = jsonencode({
    source: ["aws.s3"],
    detail: {
      eventName: ["PutObject"],
      requestParameters: {
        bucketName: [aws_s3_bucket.ingestion-bucket.bucket]
      }
    }
  })
}



resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.trigger_on_s3_bucket_change.name
  arn       = XXX-lambda-function-XXX.arn
  target_id = "lambda-target"
}


resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingestion-bucket.bucket.id
  
  eventbridge = {
    event_bridge_arn = aws_cloudwatch_event_bus.trigger_second_lambda.arn
    events = ["s3:PutObject:*"]
  }
}



resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.xxx-second-lambda-function-xxx
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.invoke_second_lambda.arn
                                                     }


