
#------------------------------------------
# Event Bridge for third lambda function 
# The code below provisions an event rule,
# event target and S3 bucket notification
# to allow the processed bucket to send 
# events when an object has been put in it
#------------------------------------------

resource "aws_cloudwatch_event_rule" "filter_for_events_from_processed_bucket"{
    name = "filters-events-created-by-putobject-in-processed-bucket"
    event_pattern = jsonencode({
    "source": ["aws.s3"],
    "detail-type": ["Object Created"],
    "detail": {
        "bucket":{
            "name": [aws_s3_bucket.processed-bucket.bucket]
            },
        "object": {
            "key": [{
                "suffix": ".parquet"
            }
            ]
        }
    }
})
}
resource "aws_cloudwatch_event_target" "target_third_lambda" {
  rule      = aws_cloudwatch_event_rule.filter_for_events_from_processed_bucket.name
  arn       = resource.aws_lambda_function.load_handler.arn  
  target_id = "ThirdLambda"
}


resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.processed-bucket.id # maybe use the bucket
  
  eventbridge = true
}



