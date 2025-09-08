# What needs to be provisioned:

# IN THE MODULE (ie here):
# 1) A lambda function DONE
# 2) An execution role for the lambda 
#    function. This includes an inline 
#    trust policy that allows lambda 
#    functions to assume this role DONE
# 3) a standalone policy to attach to the 
#    lambda execution role to let a lambda 
#    write to a bucket   DONE
# 4) a policy attachment for the policy in 3)
#     DONE  
# 5) a standalone policy to attach to the 
#    lambda execution role to let a lambda 
#    read from a bucket    DONE
# 6) a policy attachment for the policy in 5)
#     DONE
# 7) an inline resource-based policy to allow
#    a lambda to be triggered by an event 
#    from a bucket DONE
# 8) an s3 bucket (will be either ingestion 
#    or processed) with inline policy to 
#    to send events to a lambda. DONE
# 9) A CloudWatch Metric Filter that looks 
#    for "RuntimeError" in the Lambda logs.
#    A CloudWatch Alarm that fires if that 
#    filter detects errors.
# 10) An SNS Topic with an email subscription 
#    so that the project sends alerts.
# 11) asdasd asdasaasd asd asd sad sd
#    asdasd asdasaasd asd asd sad sd.








# 1) A LAMBDA FUNCTION
# ====================
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


# 2) IAM EXECUTION ROLE OF LAMBDA
# ===============================
# NOTE: "lambda_exec" below cannot
# be dynamic, ie you cannot include 
# a variable value in that string!! 
# Instead use count of for each):
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





# 3) POLICY TO LET A LAMBDA
# WRITE TO AN S3
# =========================
resource "aws_iam_policy" "lambda_put_policy" {
  count = var.should_make_s3_put_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:PutObject"],
      Resource = "arn:aws:s3:::${var.name_of_write_to_bucket}/*"  # name of bucket must actually come the bucket provisioned in this module!!
    }]
  })
                                              }


# 4) ATTACHMENT TO ATTACH THE 
# POLICY IN 3) ABOVE TO THE
# LAMBDA EXECUTION ROLE
# ===========================
resource "aws_iam_role_policy_attachment" "lambda_put_attach" {
  count      = var.should_make_s3_put_obj_policy ? 1 : 0
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_put_policy[0].arn
                                                              }



# 5) POLICY TO LET A LAMBDA
# READ FROM AN S3
# =========================
resource "aws_iam_policy" "lambda_get_policy" {
  count = var.should_make_s3_get_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:PutObject"],
      Resource = "arn:aws:s3:::${var.name_of_read_from_bucket}/*" # var will be set to an output.
    }]
  })
                                              }


# 6) ATTACHMENT TO ATTACH THE 
# POLICY IN 5) ABOVE BE ATTACHED 
# TO THE LAMBDA EXECUTION ROLE
# =============================
resource "aws_iam_role_policy_attachment" "lambda_get_attach" {
  count      = var.should_make_s3_get_obj_policy ? 1 : 0
  role       = aws_iam_role.lambda_exec.name # NOTE: this will come from an output
                                             # as the lambda exec role that reads
                                             # from a bucket will be in another
                                             # call of the module !!!!!!  
  policy_arn = aws_iam_policy.lambda_put_policy[0].arn
                                                          }





# 7) PROVISION A RESOURCE-BASED 
# INLINE POLICY TO LET A 
# LAMBDA BE TRIGGERED BY A
# BUCKET 
# =============================
# Two uses (note that the 
# extract lambda has no need 
# for this policy):
# i)  The transform lambda needs
#     this policy to let it be  
#     triggered by write to the 
#     ingestion bucket.
# ii) The load lambda needs
#     this policy to let it be 
#     triggered by writes to the 
#     processed bucket:
resource "aws_lambda_permission" "allow_s3_invoke" {
  count = var.should_make_allow_s3_invoke_policy ? 1 : 0
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mod_lambda.function_name   # <-- attachment happens here
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.my_bucket.arn # NOTE: THIS WILL COME FROM A MODULE OUTPUT
                                              # as the bucket will be provisioned in a previous 
                                              # invocation of the module.  
                                                  }



# S3 BUCKETS
# ==========
# 8) an s3 bucket (will be either ingestion 
#    or processed) with inline policies:
#    i)  one that allows it to send events
#         to a lambda (to a lambda provisioned 
#         in the next invocation of the module).

# Provision either the ingestion or 
# the processed bucket (NOTE: 
# provisioning of the code bucket 
# occurs in the root module):
resource "aws_s3_bucket" "mod-ing-or-proc-buck" {
  count  = var.should_make_ing_or_proc_bucket ? 1 : 0
  bucket = var.ing_or_proc_bucket_name
                                    }



# 8 i) Provision the notification 
# by the ingestion/processed 
# bucket to the transform/load
# lambd:
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.var.ing_or_proc_bucket_name.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.my_lambda.arn # will come from an output
                                                            # as the ingestion bkect sends
                                                            # an event to the transform
                                                            # lambda (and the processed 
                                                            # bucket sends an event to the 
                                                            # load lambda).  
    events              = ["s3:ObjectCreated:*"]  # trigger on every object PUT
                  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
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


# # 3)
# resource "aws_sns_topic" "error_alerts" {
#   name = "lambda-runtime-errors"
#                                         }

# # Subscribe your email to the topic
# resource "aws_sns_topic_subscription" "email_alert" {
#   topic_arn = aws_sns_topic.error_alerts.arn
#   protocol  = "email"
#   endpoint  = "email@example.com" 
#                                                     }

