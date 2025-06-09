#-------------------------------
# Code below provisions the third
# lambda function and associated 
# infrastructure
#-------------------------------

data "archive_file" "third_lambda_archive" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/xxxxx.py" # mon9june11.21 waiting for pull request to be granted
  output_path      = "${path.module}/../function.zip" # place to store the zip before uploading to s3
}

resource "aws_s3_object" "third_lambda_deployment" {
  bucket = aws_s3_bucket.lambda-bucket.bucket
  key    = "third_lambda/lambda.zip"
  source = data.archive_file.second_lambda_archive.output_path
}

resource "aws_lambda_function" "load_handler" {   # 11.21 mon9june25 not yet provisioned
  s3_bucket        = aws_s3_bucket.lambda-bucket.bucket
  s3_key           = aws_s3_object.third_lambda_deployment.key

  layers           = [aws_lambda_layer_version.layer.arn]

  function_name    = var.third_lambda_function
  role             = aws_iam_role.third_lambda_function_role.arn   # 11.21 mon9june25 not yet provisioned
  handler          = "third_lambda.lambda_handler" # change this to point to the handler
  runtime          = "python3.13"
  source_code_hash = data.archive_file.third_lambda_archive.output_base64sha256

#   environment {
#     variables = {
#       S3_BUCKET_NAME=aws_s3_bucket.data_bucket.bucket
#     }
#   }
}