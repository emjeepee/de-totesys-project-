#Lambda IAM role

data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "first_lambda_function_role" {
    name = "role-${var.first_lambda_function}"
    assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# Lambda IAM Policy for s3 Write

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


# Create role for second lambda to allow it to be triggered by EventBridge

#Lambda IAM role

data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "first_lambda_function_role" {
    name = "role-${var.first_lambda_function}"
    assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# Lambda IAM Policy for s3 Write

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


# Create role for second lambda to allow it to be triggered by EventBridge

# resource "aws_iam_role" "lambda_exec_role" {
#   name = "lambda-exec-role"
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17",
#     Statement = [{
#       Action = "sts:AssumeRole",
#       Principal = { Service = "lambda.amazonaws.com" },
#       Effect = "Allow",
#     }]
#   })
# }

#Define
# Refer back to trust policy, give trust policy to second lambda function
#Create
# Second function read ingestion policy
# Second function triggered by eventbridge

#Attach
# Attach read policy to role


