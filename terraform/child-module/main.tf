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
# 11) Policy to allow a lambda exec role to 
#    write to CloudWatch logs. Actually there's 
#    no separate resource for this as the 
#    attachment resource will specify an 
#    AWS-managed policy
# 12) Attachment for 11). Here set the value of 
#     the correct key to the AWS-managed policy 
#     mentioned in 11) 








# 1) A LAMBDA FUNCTION
# ====================
resource "aws_lambda_function" "mod_lambda" {
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_exec.arn
  runtime       = "python3.12"
  handler       = var.handler # format is <filename>.<function_name>
  s3_bucket     = var.code_bucket # the bucket that holds the code
  s3_key        = var.s3_key_for_zipped_lambda # site of zipped code


  layers = [
    var.layer_version_arn
    # aws_lambda_layer_version.shared-layer.arn
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
# Instead use count or for each):
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


# 11) and 12)
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                                                                   }


# 13) CREATE THE CLOUDWATCH LOG GROUP
# ===================================
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name = "/aws/lambda/${var.lambda_name}"
  retention_in_days = 14
                                                  }


# CREATE A METRIC FILTER FOR 
# THE WORD 'ERROR' IN THE LOGS
# ============================

resource "aws_cloudwatch_log_metric_filter" "lambda_error_filter" {
  name           = "${var.lambda_name}-error-count"
  log_group_name = aws_cloudwatch_log_group.lambda_logs.name # log group name for the lambda

  pattern = "\"ERROR\""  # match lines that contain the word ERROR

  metric_transformation {
    name      = "${var.lambda_name}-ErrorCount"
    namespace = "TOTESYS"
    value     = "1"  # each match increments the count by 1
  }
}


# CREATE A CLOUDWATCH ALARM
# =========================
resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  alarm_name          = "${var.lambda_name}-error-alarm" # This line ensures that a new CW metric alarm 
                                                         # gets created each time the module is called!  
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  period              = 300             # 300 seconds = 5 minutes
  metric_name         = "${var.lambda_name}-ErrorCount"
  namespace           = "TOTESYS"
  statistic           = "Sum"
  threshold           = 5
  alarm_description   = "Triggered when 5 or more errors are logged by ${var.lambda_name} within 5 minutes"

  # Use the passed-in SNS topic ARN here
  alarm_actions       = [var.sns_topic_arn]
  ok_actions          = [var.sns_topic_arn]  

# values of alarm_actions and ok_actions must be lists 
# such as 
# [var.sns_topic_arn] 
# or
#   alarm_actions = [
#   var.sns_topic_arn,
#   var.secondary_sns_topic_arn
# ]
                                                          }




# xx) SELECTIVELY PROVISION THE 
# EVENTBRIDGE SCHEDULER AND ITS
# ROLE/PERMISSION/ATTACHMENT
# NOTE: EventBridge and CloudWatch Events
# are the same thing, the latter being 
# the old name for EventBridge
# =============================

# Only create resources if 
# enable_schedule is true:

# Cloudwatch event rule:
resource "aws_cloudwatch_event_rule" "schedule" {
  count              = var.enable_EvntBrdg_res ? 1 : 0
  name               = "${var.lambda_name}-schedule"
  schedule_expression = "rate(1000 days)"
                                                }

# Tell EventBridge what to trigger
resource "aws_cloudwatch_event_target" "target" {
  count     = var.enable_EvntBrdg_res ? 1 : 0
  rule      = aws_cloudwatch_event_rule.schedule[0].name
  target_id = var.lambda_name
  arn       = aws_lambda_function.mod_lambda.arn
  role_arn  = aws_iam_role.eventbridge_invoke[0].arn
                                                }


# Permission to allow Lambda function to 
# be invoked from EventBridge scheduler:
resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.enable_EvntBrdg_res ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mod_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.schedule[0].arn
                                                    }




# Execution role that EventBridge can assume:
resource "aws_iam_role" "eventbridge_invoke" {
  count = var.enable_EvntBrdg_res ? 1 : 0
  name  = "${var.lambda_name}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
                                            }




# policy to allow the holder (which will be the 
# EventBridge execution role) to invoke the 
# first lambda function:
resource "aws_iam_policy" "invoke_lambda_policy" {
  count = var.enable_EvntBrdg_res ? 1 : 0
  name  = "${var.lambda_name}-invoke-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.mod_lambda.arn
    }]
  })
}

# Attach the policy above to the 
# EventBridge execution role:
resource "aws_iam_role_policy_attachment" "attach_policy" {
  count      = var.enable_EvntBrdg_res ? 1 : 0
  role       = aws_iam_role.eventbridge_invoke[0].name
  policy_arn = aws_iam_policy.invoke_lambda_policy[0].arn
                                                           }
# Remember that you have to use '[0]' in the two lines
# above because code created the EventBridge execution 
# role and the policy to attach to it using count, which 
# makes a list of resources, even if you only create one,
# ie aws_iam_role.eventbridge_invoke and 
# aws_iam_policy.invoke_lambda_policy both refer to 
# lists of resources (each list, in this case, containing
# just one resource). 




# ========================= ========================= =========================





# 3) POLICY TO LET A LAMBDA
# WRITE TO AN S3
# =========================
resource "aws_iam_policy" "lambda_put_policy" {
  count = var.should_make_s3_put_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-write-S3-policy"
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

  name   = "${var.lambda_name}-read-S3-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:GetObject"],
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
  policy_arn = aws_iam_policy.lambda_get_policy[0].arn
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
#     triggered by writes to the 
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
  source_arn    = var.trigger_bucket_arn
  
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
# lambda:
resource "aws_s3_bucket_notification" "bucket_notification" {
  count = var.should_make_s3_notif ? 1 : 0
  
  bucket = aws_s3_bucket.mod-ing-or-proc-buck[0].id

  lambda_function {
    
    lambda_function_arn = var.lambda_to_trigger.arn 

    events              = ["s3:ObjectCreated:*"]  # trigger on every object PUT
                  }

  depends_on = [aws_lambda_permission.allow_s3_invoke]
}








