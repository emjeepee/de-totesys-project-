#-----------------------
#First Lambda Function |
#-----------------------

data "archive_file" "second_lambda_archive" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/lambda_2.py" # change this to the file with the lambda handler in it
  output_path      = "${path.module}/../function_2.zip" # place to store the zip before uploading to s3
}

resource "aws_s3_object" "second_lambda_deployment" {
  bucket = aws_s3_bucket.lambda-bucket.bucket
  key    = "second_lambda/function.zip"
  source = data.archive_file.second_lambda_archive.output_path
}

resource "aws_lambda_function" "transform_hanlder" {
  s3_bucket        = aws_s3_bucket.lambda-bucket.bucket
  s3_key           = aws_s3_object.second_lambda_deployment.key

  layers           = [aws_lambda_layer_version.layer.arn]

  function_name    = var.second_lambda_function
  role             = aws_iam_role.second_lambda_function_role.arn
  handler          = "lambda_2.lambda_handler" # change this to point to the handler
  runtime          = var.python_runtime
  source_code_hash = data.archive_file.second_lambda_archive.output_base64sha256

#   environment {
#     variables = {
#       S3_BUCKET_NAME=aws_s3_bucket.data_bucket.bucket
#     }
#   }
}