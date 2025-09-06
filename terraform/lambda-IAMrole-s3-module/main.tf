# What needs to be provisioned:
# 1) A lambda function
# 2) An execution role for the lambda function
# 3) a standalone policy to let a lambda function
#    write to a bucket  
# 4) a policy attachment for the policy in 3)
# 5) a standalone policy to let a lambda function
#    read from a bucket  
# 6) a policy attachment for the policy in 5)
# 7) an s3 bucket with inline policy that 
#    has two statements:
#    i)   one that allows a lambda to write 
#         data to it  
#    ii)  one that allows a lambda to read 
#         data from it  
# 8) an s3 code bucket with inline policy that 
#    ???:
#    ???
# 9) A CloudWatch Metric Filter that looks for "RuntimeError" in the Lambda logs.
#    A CloudWatch Alarm that fires if that filter detects errors.
# 	 An SNS Topic with an email subscription so that the project sends alerts.









# A LAMBDA FUNCTION AND ITS
# EXECUTION ROLE
# =========================

# Provision a lambda function:
resource "aws_lambda_function" "mod_lambda" {
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_exec.arn
  runtime       = var.runtime
  handler       = var.handler # format is <filename>.<function_name>
  s3_bucket     = var.code_bucket_name # will be code bucket 
  s3_key        = var.s3_key_for_zipped_lambda # site of zipped code


  layers = [
    aws_lambda_layer_version.shared-layer.arn
           ]

  environment {
#     variables = var.environment_vars
              }
                                            }

# Provision an IAM execution role for 
# a lambda function. (NOTE: "lambda_exec"
# cannot be dynamic, ie you cannot include 
# a variable value in that string!! 
# Instead, if you need several roles just 
# like this one, use count of for each):
resource "aws_iam_role" "lambda_exec" {
  name = "${var.lambda_name}-IAM-role"

# Define the trust policy to allow 
# lambda functions to assume this 
# role (could also be standalone 
# instead of inline):
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
                                      }





# POLICY AND ATTACHMENT TO LET
# A LAMBDA WRITE TO AN S3
# =============================

# Provision the policy for 
# a lambda's execution role to 
# allow the lambda to write 
# to a bucket:
resource "aws_iam_policy" "lambda_put_policy" {
  count = var.should_make_s3_put_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:PutObject"],
      Resource = "arn:aws:s3:::${var.name_of_write_to_bucket}/*"
    }]
  })
                                              }


# Provision policy attachment
# to attach s3 put policy to
# lambda execution role:
resource "aws_iam_role_policy_attachment" "lambda_put_attach" {
  count      = var.should_make_s3_put_obj_policy_attach ? 1 : 0
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_put_policy[0].arn
                                                          }


# POLICY AND ATTACHMENT TO LET
# A LAMBDA GET FROM AN S3
# =============================

# Provision the policy for 
# a lambda execution role to 
# allow the lambda to read
# from a bucket:
resource "aws_iam_policy" "lambda_get_policy" {
  count = var.should_make_s3_get_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:GetObject"],
      Resource = "arn:aws:s3:::${var.name_of_read_from_bucket}/*"
    }]
  })
}


# Provision policy attachment
# for the policy above
resource "aws_iam_role_policy_attachment" "lambda_get_attach" {
  count      = var.should_make_s3_get_obj_policy_attach ? 1 : 0
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_get_policy[0].arn
                                                          }





# THE S3 BUCKETS
# ==============

# Provision either the ingestion or 
# the processed bucket:
resource "aws_s3_bucket" "mod-ing-or-proc-buck" {
  count  = var.should_make_ing_or_proc_bucket ? 1 : 0
  bucket = var.ing_or_proc_bucket_name
                                    }



# Provision the code bucket:
resource "aws_s3_bucket" "mod-code-buck" {
  count  = var.should_make_s3_code_bucket ? 1 : 0
  bucket = var.code_bucket_name
                                        }




# THE LOGGING AND ALARM SYSTEM
# ============================
# Provision:
# 1) a CloudWatch Metric Filter that 
# looks for "RuntimeError" in the 
# Lambda logs.
# 2) a CloudWatch Alarm that fires 
# if that filter detects errors.
# 3) an SNS Topic with an email 
# subscription so that the project 
# sends alerts.


# 3)
resource "aws_sns_topic" "error_alerts" {
  name = "lambda-runtime-errors"
                                        }

# Subscribe your email to the topic
resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.error_alerts.arn
  protocol  = "email"
  endpoint  = "email@example.com" 
                                                    }

