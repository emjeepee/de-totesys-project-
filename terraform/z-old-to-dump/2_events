
#------------------------------------------
# Event Bridge for second lambda function #|
#------------------------------------------

resource "aws_cloudwatch_event_rule" "filter_for_events_from_ingestion_bucket"{
    name = "s3PutObjectEvent"
    event_pattern = jsonencode({
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
        "bucket":{
            "name": [aws_s3_bucket.ingestion-bucket.bucket]
            },
        "object": {
            "key": [{
                "suffix": ".json"
            }
            ]
        }
    }
})
}
resource "aws_cloudwatch_event_target" "target_second_lambda" {
  rule      = aws_cloudwatch_event_rule.filter_for_events_from_ingestion_bucket.name
  arn       = resource.aws_lambda_function.transform_hanlder.arn
  target_id = "SecondLambda"
}


resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingestion-bucket.id # maybe use the bucket
  
  eventbridge = true
}







































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




