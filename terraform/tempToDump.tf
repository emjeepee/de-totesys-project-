
# Policy document for first lambda
data "aws_iam_policy_document" "allow-first-lambda-s3-put-access"{
    statement {
      actions = ["s3:PutObject"]
       resources = ["arn:aws:s3:::${aws_s3_bucket.ingestion-bucket.bucket}"]
    }
}

# Policy for first lambda
resource "aws_iam_policy" "first_lambda_policy"{
    name = "s3_put_object_access"
    policy = data.aws_iam_policy_document.allow-first-lambda-s3-put-access.json
}

# IAM role for first lambda
resource "aws_iam_role" "s3PutObjectAccess" {
    name = "give_access_to_write_to_s3_bucket"
    assume_role_policy = data.aws_iam_policy_document.allow-lambda-s3-put-access.json
  
}

# Attach the policy to the first lambda's role
resource "aws_iam_policy_attachment" "attach_policy" {
    name = "attach-policy"
    policy_arn = aws_iam_policy.first_lambda_policy.arn
    roles = aws_iam_role.s3PutObjectAccess
  
}


# Create role for second lambda to allow it to be triggered by EventBridge
resource "aws_iam_role" "lambda_exec_role" {
  name = "lambda-exec-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Principal = { Service = "lambda.amazonaws.com" },
      Effect = "Allow",
    }]
  })
}
