
# Set two module outputs, one for bucket
# name, the other for the bucket's arn:
output "bucket_name" {
  value = aws_s3_bucket.mod-buck.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.mod-buck.arn
}

# is this needed?:
output "lambda_arn" {
  value = aws_lambda_function.mod_lambda.arn
                    }