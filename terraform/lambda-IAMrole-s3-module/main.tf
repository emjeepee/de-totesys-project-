
# IAM execution role for 
# the lambda function:
resource "aws_iam_role" "lambda_exec" {
  name = "${var.lambda_name}-IAM-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}




# Provision the lambda function
# itself:
resource "aws_lambda_function" "mod_lambda" {
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_exec.arn
  runtime       = var.runtime
  handler       = var.handler
  s3_bucket     = var.s3_bucket
  s3_key        = var.s3_function_key

  environment {
#     variables = var.environment_vars
              }
                                            }

# is this needed?:
output "lambda_arn" {
  value = aws_lambda_function.mod_lambda.arn
                    }