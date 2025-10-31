# WHAT NEEDS TO BE PROVISIONED:
# =============================
# IN THE CHILD MODULE:
# --------------------
# 1) A lambda function
# 2) An execution role for the lambda 
#    function
# 3) a standalone policy to attach to the 
#    lambda execution role to let a lambda 
#    write to a bucket  
# 4) a policy attachment for the policy in 3)
# 5) a standalone policy to attach to the 
#    lambda execution role to let a lambda 
#    read from a bucket   
# 6) a policy attachment for the policy in 5)
# 7) an inline resource-based policy to allow
#    a lambda to be triggered by an event 
#    from a bucket
# 8) an s3 bucket with inline policy that
#    allows it to send events to a lambda
#    (will be either ingestion bucket and
#    policy to send events to transform 
#    lambda or processed bucket and policy to 
#    send events to load lambda).
# 9) A CloudWatch Metric Filter that looks 
#    for "Error" in the Lambda logs.
#    A CloudWatch Alarm that fires if that 
#    filter detects some number of errors
#    over some time period.
# 10) An SNS Topic with an email subscription 
#    so that the project sends alerts.
# 11) asdasd asdasaasd asd asd sad sd
#    asdasd asdasaasd asd asd sad sd.


# IN THE ROOT MODULE (ie here):
# -----------------------------
# 1) The provider
# 2) The EvenBridge Scheduler to trigger
#    the extract lambda at some frequency.
#    Also: an IAM execution role for the 
#    Scheduler, a policy for it and a
#    policy attachment.
# 2) The lambda permission to allow the
#    extract lambda to be invoked by 
#    EventBridge Scheduler.
# 2) the backend bucket (to hold the 
#    state file)
# 3) an s3 bucket to store the zipped lambda 
#    handlers and the zipped layer
# 4) A policy that allows the code S3 bucket 
#    to be read by the AWS Lambda service (so
#    that the SERVICE (not the lambda 
#    function) can get the zipped handler and
#    layer files.
# 5) the loading of a zipped lambda handler code
# 6) the loading of the zipped layer file
# 7) the lambda layer version
# 8) in turn (via invocations of the module):
#    a) the extract lambda, lambda exec role,
#       role policy for writing to ingestion 
#       bucket, attachment for that policy,
#       ingestion bucket
#    b) the transform lambda, lambda exec role,
#       role policy for reading from ingestion
#       bucket, attachment for that policy,
#       policy to write to processed bucket, 
#       attachment for that policy, processed 
#       bucket
#    c) the load lambda, policy to read from 
#       processed bucket






# 1):
terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 5.0"
          }
                     }


# Provision the S3 bucket that will
# contain Terraform state file.backend 
# (the 'a9fk3l2q' in the bucket name 
# is a random 8-char substring to
# ensure name is unique worldwide
# within an AWS partition -- all 
# buckets in this project are 
# named this way):
  backend "s3" {
  bucket = "totesys-state-bucket-a9fk3l2q"
    key = "terraform/state.tfstate"
    region = "eu-west-2"
               }
         }


provider "aws" {
  region = "eu-west-2"
               }








# THE CODE BUCKET 
# ===============
resource "aws_s3_bucket" "code_bucket" {
  # bucket = "totesys-code-bucket-m1x7qr0b"
    bucket = var.code_bucket
                                       }

# POLICY TO LET CODE BUCKET BE 
# READ BY THE AWS LAMBDA SERVICE 
# =============================
resource "aws_s3_bucket_policy" "AWS_lambda_SERVICE_access" {
  bucket = aws_s3_bucket.code_bucket.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid: "AllowLambdaServiceToReadCode",
        Effect: "Allow",
        Principal: {
          Service: "lambda.amazonaws.com"
        },
        Action: [
          "s3:GetObject"
        ],
        Resource: "${aws_s3_bucket.code_bucket.arn}/*"
      }
    ]
  })
}




# LOAD THE ZIPPED LAYER FILE AND 
# THE ZIPPED LAMBDA FUNCTION 
# HANDLERS INTO THE CODE BUCKET
#===============================

# zipped layer file: 
resource "aws_s3_object" "layer_zip" {
  bucket = "totesys-code-bucket-m1x7qr0b" # name of the code bucket
  key    = "zipped/layer.zip"
  source = "../zipped_files/layer.zip"
                                     }

# zipped handler for the first
# (extract) lambda: 
resource "aws_s3_object" "first_lambda_zip" {
  bucket = "totesys-code-bucket-m1x7qr0b"
  key    = "zipped/first_lambda.zip"
  source = "../zipped_files/first_lambda.zip" # must be relative to 
                                              # the directory that contains
                                              # this file we are in, ie 
                                              # directory terraform
                                            }

# zipped handler for the second
# (transform) lambda: 
resource "aws_s3_object" "second_lambda_zip" {
  bucket = "totesys-code-bucket-m1x7qr0b"
  key    = "zipped/second_lambda.zip"
  source = "../zipped_files/second_lambda.zip"  # must be relative to terraform dir
                                              }

# zipped handler for the third
# (load) lambda: 
resource "aws_s3_object" "third_lambda_zip" {
  bucket = "totesys-code-bucket-m1x7qr0b"
  key    = "zipped/third_lambda.zip"
  source = "../zipped_files/third_lambda.zip"  # must be relative to terraform dir
                                            }






# CREATE LAMBDA LAYER VERSION
# (SHARED BY THE THREE LAMBDAS)
# =============================
resource "aws_lambda_layer_version" "shared-layer" {
  layer_name          = "layer-shared-by_all_lambdas"
  s3_bucket           = "totesys-code-bucket-m1x7qr0b"
  s3_key              = aws_s3_object.layer_zip.key
  compatible_runtimes = ["python3.13"]
                                                  }


# CREATE AN SNS TOPIC AND 
# EMAIL SUBSCRIPTION
# =======================
resource "aws_sns_topic" "lambda_error_topic" {
  name = "lambda-error-alerts"
                                              }

resource "aws_sns_topic_subscription" "lambda_error_email" {
  topic_arn = aws_sns_topic.lambda_error_topic.arn
  protocol  = "email"
  endpoint  = "mukund.panditman@googlemail.com" 
                                                           }







# ============================================================================
# ============================================================================




# CALLS OF THE MODULE
# ===================


# 8) in turn (via invocations of the module):
#    a) the extract lambda, lambda exec role,
#       role policy for writing to ingestion 
#       bucket, attachment for that policy,
#       ingestion bucket
#    b) the transform lambda, lambda exec role,
#       role policy for reading from ingestion
#       bucket, attachment for that policy,
#       policy to write to processed bucket, 
#       attachment for that policy, processed 
#       bucket
#    c) the load lambda, policy to read from 
#       processed bucket


# FIRST LAMBDA
# The first call of the module will provision:
# 1) the first lambda
# 2) the first lambda exec role
# 3) the policy to let a lambda write to the
#    ingestion bucket 
# 4) the attachment for the policy
# 5) an s3 bucket (ingestion)

# VARS TO SRET:
# lambda_name
# runtime
# handler
# code_bucket_name
# s3_key_for_zipped_lambda
# should_make_s3_put_obj_policy
# name_of_write_to_bucket
# should_make_ing_or_proc_bucket
# ing_or_proc_bucket_name
# 


module "extract" {
  # The following are common to all Lambdas, 
  # hence are set in the child module file,
  # not here:
  # code_bucket                          = "totesys-code-bucket-m1x7qr0b"
  # runtime                              = "python3.13"


  # vars that are not set here 
  # are false by default
  source                               = "../child-module/"  # has to be a folder
  code_bucket                          = var.code_bucket
  # for first lambda function:
  lambda_name                          = "extract-lambda"
  handler                              = "first_lambda_handler.first_lambda_handler"
  s3_key_for_zipped_lambda             = "zipped/first_lambda.zip"
  layer_version_arn                    = aws_lambda_layer_version.shared-layer.arn
  enable_EvntBrdg_res                  = true

  # for lambda exec role policy 
  # that allows lambda to write
  # to ingestion bucket:
  name_of_write_to_bucket              = "totesys-ingestion-bucket-t8vl5n6p"


  # for ingestion bucket:
  ing_or_proc_bucket_name              = "totesys-ingestion-bucket-t8vl5n6p"

  # conditional vars:
  should_make_ing_or_proc_bucket       = true

  # this module will not use the 
  # following variable but it must 
  # be set:
  name_of_read_from_bucket             = ""

  # this module's lambda will not 
  # be triggered by an S3 bucket:
  should_make_allow_s3_invoke_policy   = false

  # The lambda of this invocation 
  # of the module does not need
  # the arn of the bucket provisioned 
  # in the previous invocation of the 
  # module because there was no 
  # previous invoaction of the module
  trigger_bucket_arn = "not-needed"

  # The extract lambda of this invocation 
  # must provision an S3 notification
  # to llow the ingestion bucket to 
  # trigger the transform lambda:
  should_make_s3_notif = true


  # The transform lambda must be
  # triggered by the ingestion bucket:
  lambda_to_trigger = module.transform.lambda_to_trigger

  # the sns topic:
  sns_topic_arn = aws_sns_topic.lambda_error_topic.arn

                                        }







# SECOND LAMBDA
# The 2nd call of the module will provision:
# 1) the 2nd lambda
# 2) the 2nd lambda exec role
# 3) the exec role's policy to let a lambda 
#    read from a bucket 
# 4) the attachment for 3)
# 5) the policy to let a lambda write to a bucket 
#    (the processed bucket)
# 6) the attachment for 5)
# 7) an s3 bucket (processed)


module "transform" {
  # The following are common to all Lambdas, 
  # hence are set in the child module file,
  # not here:
  # runtime                              = "python3.13"


  # vars not set here 
  # are default false
  source                               = "./lambda-IAMrole-s3-module"

  # for second lambda function:
  lambda_name                          = "transform-lambda"
  handler                              = "second_lambda_handler.second_lambda_handler"
  s3_key_for_zipped_lambda             = "zipped/second_lambda.zip"
  layer_version_arn                    = aws_lambda_layer_version.shared-layer.arn
  enable_EvntBrdg_res                  = false
  code_bucket                          = var.code_bucket

  # for lambda exec role policy 
  # that allows lambda to read
  # from ingestion bucket
  # (module.<MODULE-NAME>.<OUTPUT-NAME>
  # will have value 
  # "totesys-ingestion-bucket-t8vl5n6p"):
  name_of_read_from_bucket             = module.extract.name_of_bckt_that_triggers_next_lambda # an output


  # for lambda exec role policy 
  # that allows lambda to read
  # from ingestion bucket:
  name_of_write_to_bucket             = "totesys-processed-bucket-h2z4ks9w"


  # for processed bucket:
  # (STILL NEEDED?):
  ing_or_proc_bucket_name              = "totesys-processed-bucket-h2z4ks9w"

  # conditional vars (value of a var 
  # not set here is false by default):
  should_make_ing_or_proc_bucket       = true
  should_make_s3_put_obj_policy        = true # for put obj into proc bucket
  should_make_s3_get_obj_policy        = true # for get obj from ing bucket

  # this module's lambda will  
  # be triggered by an S3 bucket:
  should_make_allow_s3_invoke_policy   = true

  # The lambda of this invocation 
  # of the child module needs the arn of 
  # the bucket provisioned in the
  # previous invocation of the child module 
  trigger_bucket_arn = module.extract.trigger_bucket_arn 

  # This invocation of th child module
  # must provision an S3 notification
  # to allow the processed bucket to 
  # trigger the load lambda:
  should_make_s3_notif = true


  # The load lambda must be
  # triggered by the processed bucket
  # (module.load.lambda_to_trigger means 
  # the value of the output of the load 
  # module):
  lambda_to_trigger = module.load.lambda_to_trigger

  # the sns topic:
  sns_topic_arn = aws_sns_topic.lambda_error_topic.arn

                                }







# The 3rd call of the module will provision:
# 1) the 3rd lambda
# 2) the 3rd lambda exec role
# 3) the policy to let a lambda read from 
#    the processed bucket 
# 4) the attachment for 3)

module "load" {


  # vars not set here are 
  # default false
  source                               = "./lambda-IAMrole-s3-module"

  # for third lambda function:
  lambda_name                          = "load-lambda"
  handler                              = "third_lambda_handler.third_lambda_handler"
  s3_key_for_zipped_lambda             = "zipped/third_lambda.zip"
  layer_version_arn                    = aws_lambda_layer_version.shared-layer.arn
  enable_EvntBrdg_res                  = false
  code_bucket                          = var.code_bucket

  # No need to provision a bucket:
  should_make_ing_or_proc_bucket       = false

  # No needed for this call 
  # of the module but you 
  # still need to set it:
  name_of_write_to_bucket              = "not-needed"

  # for lambda exec role policy 
  # that allows lambda to read
  # from processed bucket:
  name_of_read_from_bucket             = module.transform.name_of_bckt_that_triggers_next_lambda

  # for processed bucket.
  # not needed:
  ing_or_proc_bucket_name              = "not-needed"

  # conditional vars:
  should_make_s3_get_obj_policy        = true # for get obj from proc bucket
  
  # The lambda of this invocation 
  # of the child module needs the arn of 
  # the bucket provisioned in the
  # previous invocation of the child module 
  # (ie the processed bucket)
  trigger_bucket_arn = module.transform.trigger_bucket_arn 


  # No S3 bucket will trigger this module:
  should_make_allow_s3_invoke_policy   = true


  # This invocation of the child module
  # must not provision an S3 notification
  should_make_s3_notif = false


  # This invocation of the child module
  # provisions no bucket, so there is
  # no lambda to trigger:
  lambda_to_trigger = "not-needed"

  # the sns topic:
  sns_topic_arn = aws_sns_topic.lambda_error_topic.arn

                                }
