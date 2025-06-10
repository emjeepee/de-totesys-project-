# Create role for third lambda to allow it to be triggered by EventBridge

#Define
# Refer back to trust policy, give trust policy to second lambda function
#Create
# Third function read ingestion policy
# Third function triggered by eventbridge

#Attach
# Attach read policy to role

#Can just copy paste iams for first function changing var to Third function



#------------------------------------
#Lambda IAM role for the third lambda|
#------------------------------------


# policy document same as the one in iam.tf file

resource "aws_iam_role" "third_lambda_function_role" {
    name = "role-${var.third_lambda_function}-name"
    assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

resource "aws_iam_role_policy_attachment" "third_lambda_cw_policy_attachment" {
    role = aws_iam_role.third_lambda_function_role.name
    policy_arn = aws_iam_policy.third_cw_policy.arn
}
#--------------------------------
# Cloud Watch for third Lambda #
#--------------------------------

resource "aws_lambda_permission" "allow_3rd_lambda" {
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.load_handler.function_name # change to second lambda
  principal = "events.amazonaws.com"
  source_arn = aws_cloudwatch_event_rule.filter_for_events_from_processed_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

# ---------------------------------
# Third Lambda IAM policy for S3 |
# ---------------------------------
data "aws_iam_policy_document" "s3_access_policy_doc_for_3rd_lambda"{
  statement {
    actions = ["s3:Get*", "s3:List*"]
    resources = [
        "arn:aws:s3:::${var.processed-bucket}",
        "arn:aws:s3:::${var.processed-bucket}/*"
    ]
   }
  statement {
    effect    = "Allow"
    actions   = ["lambda:GetLayerVersion"]
    resources = ["arn:aws:lambda:*:*:layer:*:*"]
  }
}

resource "aws_iam_policy" "s3_access_policy_for_3rd_lambda" {
  name = "s3-policy-for-${var.third_lambda_function}"
  policy = data.aws_iam_policy_document.s3_access_policy_doc_for_3rd_lambda.json
}

resource "aws_iam_role_policy_attachment" "third_lambda_s3_write_policy_attachment" {
  role = aws_iam_role.third_lambda_function_role.name
  policy_arn = aws_iam_policy.s3_access_policy_for_3rd_lambda.arn
}









# ----------------------------------
# Lambda IAM Policy for Cloudwatch |
# ---------------------------------

data "aws_iam_policy_document" "third_cloudwatch_document"{
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
        resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.third_lambda_function}:*"]
    }
}

resource "aws_iam_policy" "third_cw_policy"{
    name = "cw-policy-${var.third_lambda_function}-cloudwatch"
    description = "Cloudwatch logging policy for third lambda function"
    policy = data.aws_iam_policy_document.third_cloudwatch_document.json
}