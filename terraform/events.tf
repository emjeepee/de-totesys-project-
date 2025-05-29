#---------------------------------------
#Event Bridge for first lambda function|
#--------------------------------------

# resource "aws_cloudwatch_event_rule" "scheduler" {
#   name =  "extract-handler-5-minutes"
#   schedule_expression = "rate(5 minutes)"
# }

# resource "aws_cloudwatch_event_target" "lambda_target" {
#   rule = aws_cloudwatch_event_rule.scheduler.name
#   arn = aws_lambda_function.extract_handler.arn
# }

# resource "aws_lambda_permission" "allow_scheduler" {
#   action = "lambda:InvokeFunction"
#   function_name = aws_lambda_function.extract_handler.function_name
#   principal = "events.amazonaws.com"
#   source_arn = aws_cloudwatch_event_rule.scheduler.arn
#   source_account = data.aws_caller_identity.current.account_id
# }














































# # Create a custom event bus
# resource "aws_cloudwatch_event_bus" "bus_for_events_from_ingestion_S3_bucket"{
#     name = "bus_for_events_from_ingestion_S3_bucket"
# }


# # Create an event rule (to filter for events put on the event bus above
# # by the ingestion S3 bucket)
# resource "aws_cloudwatch_event_rule" "filter_for_events_from_ingestion_bucket" {
#   name        = "filter_for_events_from_ingestion_bucket"
#   description = "Filter for events put on the EventBridge bus by the ingestion S3 bucket when files are modified in the bucket or added to the bucket"
#   event_bus_name = aws_cloudwatch_event_bus.bus_for_events_from_ingestion_S3_bucket.name

#   event_pattern = jsonencode({
#     source: ["aws.s3"],
#     detail: {
#       eventName: ["PutObject"],
#       requestParameters: {
#         bucketName: [aws_s3_bucket.ingestion-bucket.bucket]
#       }
#     }
#   })
# }


# # Set the target (the second lambda) for the rule created above
# resource "aws_cloudwatch_event_target" "target_second_lambda" {
#   rule      = aws_cloudwatch_event_rule.filter_for_events_from_ingestion_bucket.name
#   arn       = XXX-lambda-function-XXX.arn
#   target_id = "lambda-target"
# }


# #  Allow the ingestion S3 bucket to make notifications
# # that will go onto the custom bus created above
# # when files in the ingestion bucket are added to it or
# # when files already in it are modified
# resource "aws_s3_bucket_notification" "bucket_notification" {
#   bucket = aws_s3_bucket.ingestion-bucket.bucket.id
  
#   eventbridge = {
#     event_bridge_arn = aws_cloudwatch_event_bus.trigger_second_lambda.arn
#     events = ["s3:PutObject:*"]
#   }
# }



