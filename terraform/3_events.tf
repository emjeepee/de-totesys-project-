# # Create a custom event bus
resource "aws_cloudwatch_event_bus" "bus_for_events_from_processed_S3_bucket"{
    name = "bus_for_events_from_processed_S3_bucket"
}

# Create an event rule (to filter for events put on the event bus above
# by the ingestion S3 bucket)
resource "aws_cloudwatch_event_rule" "filter_for_events_from_processed_bucket"{
    name = "s3PutObjectEvent"
    event_bus_name = aws_cloudwatch_event_bus.bus_for_events_from_processed_S3_bucket.name
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


# Set the target (the third lambda) for the rule created above
resource "aws_cloudwatch_event_target" "target_third_lambda" {
  rule      = aws_cloudwatch_event_rule.filter_for_events_from_processed_bucket.name
  arn       = resource.aws_lambda_function.xxx3rd-lambda-terraform-name-herexxx-.arn # REPLACE with real Terraform name of 3rd lambda
  target_id = "ThirdLambda"
}


#  Allow the processed S3 bucket to make notifications
# that will go onto the custom bus created above
# when files in the processed bucket are added to it or
# when files already in it are modified
resource "aws_s3_bucket_notification" "processed_bucket_notification" {
  bucket = aws_s3_bucket.processed-bucket.id # maybe use the bucket
  
  eventbridge = true
}

