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

# I had previously provisioned the state
# bucket here but you have to expose 
# the name of the bucket here because
# you can't use a 
# env-var+terraform-variable for the 
# value of key 'bucket' below (you 
# would have to use the actual name of 
# the bucket):
  # backend "s3" {
  # # bucket = ""
  # bucket = var.AWS_STATE_BUCKET
  #   key = "terraform/state.tfstate"
  #   region = "eu-west-2"
  #              }
         }


provider "aws" {
  region = "eu-west-2"
               }








# THE CODE BUCKET 
# ===============
resource "aws_s3_bucket" "code_bucket" {
  # bucket = "totesys-code-bucket-m1x7qr0b"
    bucket = var.AWS_CODE_BUCKET
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

# Put the zipped layer file
# in the code bucket: 
resource "aws_s3_object" "layer_zip" {
  bucket = var.AWS_CODE_BUCKET
  key    = "zipped/layer.zip"
  source = "../../zipped_files/layer.zip"
                                     }

# zipped handler for the first
# (extract) lambda: 
resource "aws_s3_object" "first_lambda_zip" {
  bucket = var.AWS_CODE_BUCKET
  key    = "zipped/first_lambda.zip"
  source = "../../zipped_files/first_lambda.zip" # must be relative to 
                                              # the directory that contains
                                              # this file we are in, ie 
                                              # directory terraform/root-module
                                            }

# zipped handler for the second
# (transform) lambda: 
resource "aws_s3_object" "second_lambda_zip" {
  bucket = var.AWS_CODE_BUCKET
  key    = "zipped/second_lambda.zip"
  source = "../../zipped_files/second_lambda.zip"  # must be relative to terraform dir
                                              }

# zipped handler for the third
# (load) lambda: 
resource "aws_s3_object" "third_lambda_zip" {
  bucket = var.AWS_CODE_BUCKET
  key    = "zipped/third_lambda.zip"
  source = "../../zipped_files/third_lambda.zip"  # must be relative to terraform dir
                                            }






# CREATE LAMBDA LAYER VERSION
# (SHARED BY THE THREE LAMBDAS)
# =============================
resource "aws_lambda_layer_version" "shared-layer" {
  layer_name          = "layer-shared-by_all_lambdas"
  s3_bucket           = var.AWS_CODE_BUCKET
  s3_key              = aws_s3_object.layer_zip.key
  compatible_runtimes = ["python3.12"]
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
  endpoint  = var.AWS_ALERT_EMAIL
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




module "extract" {

  TOTE_SYS_DB_DB = var.TOTE_SYS_DB_DB
  TOTE_SYS_DB_HOST = var.TOTE_SYS_DB_HOST
  TOTE_SYS_DB_PASSWORD = var.TOTE_SYS_DB_PASSWORD
  TOTE_SYS_DB_PORT = var.TOTE_SYS_DB_PORT
  TOTE_SYS_DB_USER = var.TOTE_SYS_DB_USER

  WAREHOUSE_DB_DB = var.WAREHOUSE_DB_DB
  WAREHOUSE_DB_HOST = var.WAREHOUSE_DB_HOST
  WAREHOUSE_DB_PASSWORD = var.WAREHOUSE_DB_PASSWORD
  WAREHOUSE_DB_PORT = var.WAREHOUSE_DB_PORT
  WAREHOUSE_DB_USER = var.WAREHOUSE_DB_USER

  AWS_INGEST_BUCKET = var.AWS_INGEST_BUCKET
  AWS_PROCESS_BUCKET = var.AWS_PROCESS_BUCKET
  AWS_ALERT_EMAIL = var.AWS_ALERT_EMAIL
  AWS_CODE_BUCKET = var.AWS_CODE_BUCKET
  AWS_TABLES_LIST = var.AWS_TABLES_LIST

  OLTP_NAME                  = var.OLTP_NAME
  WAREHOUSE_NAME             = var.WAREHOUSE_NAME

  # conditionally used to set the 
  # permissions for the Lambdas to 
  # access the ingestion/processed 
  # bucket:
  stage = "extract" 

  # vars that are not set here 
  # are false by default
  source                               = "../child-module/"  # has to be a folder
  # for first lambda function:
  lambda_name                          = "extract-lambda"
  handler                              = "first_lambda_handler.first_lambda_handler"
  s3_key_for_zipped_lambda             = "zipped/first_lambda.zip"
  layer_version_arn                    = aws_lambda_layer_version.shared-layer.arn
  enable_EvntBrdg_res                  = true

  # for lambda exec role policy 
  # that allows lambda to write
  # to ingestion bucket:
  name_of_write_to_bucket              = var.AWS_INGEST_BUCKET


  # for ingestion bucket:
  ing_or_proc_bucket_name              = var.AWS_INGEST_BUCKET

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

  TOTE_SYS_DB_DB = var.TOTE_SYS_DB_DB
  TOTE_SYS_DB_HOST = var.TOTE_SYS_DB_HOST
  TOTE_SYS_DB_PASSWORD = var.TOTE_SYS_DB_PASSWORD
  TOTE_SYS_DB_PORT = var.TOTE_SYS_DB_PORT
  TOTE_SYS_DB_USER = var.TOTE_SYS_DB_USER

  WAREHOUSE_DB_DB = var.WAREHOUSE_DB_DB
  WAREHOUSE_DB_HOST = var.WAREHOUSE_DB_HOST
  WAREHOUSE_DB_PASSWORD = var.WAREHOUSE_DB_PASSWORD
  WAREHOUSE_DB_PORT = var.WAREHOUSE_DB_PORT
  WAREHOUSE_DB_USER = var.WAREHOUSE_DB_USER

  AWS_CODE_BUCKET = var.AWS_CODE_BUCKET
  AWS_INGEST_BUCKET = var.AWS_INGEST_BUCKET
  AWS_PROCESS_BUCKET = var.AWS_PROCESS_BUCKET

  AWS_ALERT_EMAIL = var.AWS_ALERT_EMAIL
  
  AWS_TABLES_LIST = var.AWS_TABLES_LIST

  OLTP_NAME                  = var.OLTP_NAME
  WAREHOUSE_NAME             = var.WAREHOUSE_NAME

  # conditionally used to set the 
  # permissions for the Lambdas to 
  # access the ingestion/processed 
  # bucket:
  stage = "transform" 

  # vars not set here 
  # are default false
  source                               = "../child-module/" # has to be a folder

  # for second lambda function:
  lambda_name                          = "transform-lambda"
  handler                              = "second_lambda_handler.second_lambda_handler"
  s3_key_for_zipped_lambda             = "zipped/second_lambda.zip"
  layer_version_arn                    = aws_lambda_layer_version.shared-layer.arn
  enable_EvntBrdg_res                  = false
  # code_bucket                          = var.AWS_CODE_BUCKET

  # for lambda exec role policy 
  # that allows lambda to read
  # from ingestion bucket
  # (module.<MODULE-NAME>.<OUTPUT-NAME>
  name_of_read_from_bucket             = var.AWS_INGEST_BUCKET
  # previously I used this:
  # name_of_read_from_bucket             = module.extract.name_of_bckt_that_triggers_next_lambda # an output


  # for lambda exec role policy 
  # that allows lambda to read
  # from ingestion bucket:
  name_of_write_to_bucket             = var.AWS_PROCESS_BUCKET


  # for processed bucket:
  # (STILL NEEDED?):
  ing_or_proc_bucket_name              = var.AWS_PROCESS_BUCKET

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

  TOTE_SYS_DB_DB = var.TOTE_SYS_DB_DB
  TOTE_SYS_DB_HOST = var.TOTE_SYS_DB_HOST
  TOTE_SYS_DB_PASSWORD = var.TOTE_SYS_DB_PASSWORD
  TOTE_SYS_DB_PORT = var.TOTE_SYS_DB_PORT
  TOTE_SYS_DB_USER = var.TOTE_SYS_DB_USER

  WAREHOUSE_DB_DB = var.WAREHOUSE_DB_DB
  WAREHOUSE_DB_HOST = var.WAREHOUSE_DB_HOST
  WAREHOUSE_DB_PASSWORD = var.WAREHOUSE_DB_PASSWORD
  WAREHOUSE_DB_PORT = var.WAREHOUSE_DB_PORT
  WAREHOUSE_DB_USER = var.WAREHOUSE_DB_USER

  AWS_CODE_BUCKET = var.AWS_CODE_BUCKET
  AWS_INGEST_BUCKET = var.AWS_INGEST_BUCKET
  AWS_PROCESS_BUCKET = var.AWS_PROCESS_BUCKET

  AWS_ALERT_EMAIL = var.AWS_ALERT_EMAIL
  AWS_TABLES_LIST = var.AWS_TABLES_LIST

  OLTP_NAME                  = var.OLTP_NAME
  WAREHOUSE_NAME             = var.WAREHOUSE_NAME

  # conditionally used to set the 
  # permissions for the Lambdas to 
  # access the ingestion/processed 
  # bucket:
  stage = "load" 

  # vars not set here are 
  # default false
  source                               = "../child-module/" # has to be a folder

  # for third lambda function:
  lambda_name                          = "load-lambda"
  handler                              = "third_lambda_handler.third_lambda_handler"
  s3_key_for_zipped_lambda             = "zipped/third_lambda.zip"
  layer_version_arn                    = aws_lambda_layer_version.shared-layer.arn
  enable_EvntBrdg_res                  = false
  
  # No need to provision a bucket:
  should_make_ing_or_proc_bucket       = false

  # No needed for this call 
  # of the module but you 
  # still need to set it:
  name_of_write_to_bucket              = "not-needed"

  # for lambda exec role policy 
  # that allows lambda to read
  # from processed bucket:

  name_of_read_from_bucket            =  var.AWS_PROCESS_BUCKET
  # previously I had this:
  # name_of_read_from_bucket             = module.transform.name_of_bckt_that_triggers_next_lambda

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
