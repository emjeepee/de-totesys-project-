data "aws_iam_policy_document" "allow-lambda-s3-put-access"{
    statement {
      actions = ["s3:PutObject"]
       resources = ["arn:aws:s3:::${aws_s3_bucket.ingestion-bucket.bucket}"]
    }
}
resource "aws_iam_policy" "first_lambda_policy"{
    name = "s3_put_object_access"
    policy = data.aws_iam_policy_document.allow-lambda-s3-put-access.json
}


resource "aws_iam_role" "s3PutObjectAccess" {
    name = "give_access_to_write_to_s3_bucket"
    assume_role_policy = data.aws_iam_policy_document.allow-lambda-s3-put-access.json
  
}

resource "aws_iam_policy_attachment" "attach_policy" {
    name = "attach-policy"
    policy_arn = aws_iam_policy.first_lambda_policy.arn
    roles = aws_iam_role.s3PutObjectAccess
  
}

