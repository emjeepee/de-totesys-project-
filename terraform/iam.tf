#-----------------
#Lambda IAM role |
#----------------

data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
  statement {
    effect    = "Allow"
    actions   = ["lambda:GetLayerVersion"]
    resources = ["arn:aws:lambda:*:*:layer:*:*"]
  }
}

resource "aws_iam_role" "first_lambda_function_role" {
    name = "role-${var.first_lambda_function}"
    assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

#---------------------------------
# Lambda IAM Policy for s3 Write |
#--------------------------------

data "aws_iam_policy_document" "s3_ingestion_data_policy_doc"{
  statement {
    actions = ["s3:Get*", "s3:List*"]
    resources = [
        "arn:aws:s3:::${var.ingestion-bucket}",
        "arn:aws:s3:::${var.ingestion-bucket}/*"
    ]
  }
  statement {
    actions = ["s3:Put*"]
    resources = [
        "arn:aws:s3:::${var.ingestion-bucket}",
        "arn:aws:s3:::${var.ingestion-bucket}/*"
    ]
  }
}

resource "aws_iam_policy" "s3_write_policy" {
  name = "s3-policy-${var.first_lambda_function}-write"
  policy = data.aws_iam_policy_document.s3_ingestion_data_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "first_lambda_s3_write_policy_attachment" {
  role = aws_iam_role.first_lambda_function_role.name
  policy_arn = aws_iam_policy.s3_write_policy.arn
}

# ----------------------------------
# Lambda IAM Policy for Cloudwatch |
# ---------------------------------

data "aws_iam_policy_document" "first_cloudwatch_document"{
    statement{
        actions = ["logs:CreateLogGroup"]
        resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*:*"]
    }

    statement {
        actions = [
            "logs:CreateLogStream",
            "logs:PutLogEvents",
            "logs:PutLogEventsBatch",
            ]
        resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.first_lambda_function}:*"]
    }
}

resource "aws_iam_policy" "cw_policy"{
    name = "cw-policy-${var.first_lambda_function}"
    description = "Cloudwatch logging policy for first lambda function"
    policy = data.aws_iam_policy_document.first_cloudwatch_document.json
}

resource "aws_iam_role_policy_attachment" "first_lambda_cw_policy_attachment" {
    role = aws_iam_role.first_lambda_function_role.name
    policy_arn = aws_iam_policy.cw_policy.arn
}

# Create role for second lambda to allow it to be triggered by EventBridge

#Define
# Refer back to trust policy, give trust policy to second lambda function
#Create
# Second function read ingestion policy
# Second function triggered by eventbridge

#Attach
# Attach read policy to role


