#-------------------------------
# Code below provisions the third
# lambda function and associated 
# infrastructure
#-------------------------------

data "archive_file" "third_lambda_archive" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/third_lambda.py"
  output_path      = "${path.module}/../function_3.zip" # place to store the zip before uploading to s3
}

resource "aws_s3_object" "third_lambda_deployment" {
  bucket = aws_s3_bucket.lambda-bucket.bucket
  key    = "third_lambda/lambda.zip"
  source = data.archive_file.third_lambda_archive.output_path
}

resource "aws_lambda_function" "load_handler" {   
  s3_bucket        = aws_s3_bucket.lambda-bucket.bucket
  s3_key           = aws_s3_object.third_lambda_deployment.key

  layers           = [aws_lambda_layer_version.layer.arn]

  function_name    = var.third_lambda_function
  role             = aws_iam_role.third_lambda_function_role.arn  
  handler          = "third_lambda.lambda_handler" # points to the handler function itself
  runtime          = var.python_runtime
  source_code_hash = data.archive_file.third_lambda_archive.output_base64sha256

#   environment {
#     variables = {
#       S3_BUCKET_NAME=aws_s3_bucket.data_bucket.bucket
#     }
#   }
}