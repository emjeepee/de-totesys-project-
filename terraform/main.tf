terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 5.0"
          }
                     }


# Provision the S3 bucket that will
# contain Terraform state file.backend 
# FROM AWS: General purpose 
# bucket names can consist 
# only of lowercase letters, 
# numbers, periods (.), and 
# hyphens (-).
# (the 'a9f.k3-l2q' in the bucket name 
# is a random 10-char substring to
# ensure name is unique worldwide
# within an AWS partition -- all 
# buckets in this project are 
# named this way):
  backend "s3" {
    bucket = "totesys-state-bucket-a9f.k3-l2q"
    key = "state.tf"
    region = "eu-west-2"
              }
         }



provider "aws" {
  region = "eu-west-2"
               }




# FIRST LAMBDA
# The first call of the module will provision:
# 1) the first lambda
# 2) the first lambda exec role
# 3) the policy to let a lambda write to a bucket 
# 4) the attachment for the policy
# 5) an s3 bucket (ingestion) with inline policy 
#    statements that allow the first lambda to 
#    write to it and the second lambda to read 
#    from it
# 6) The code bucket

module "lambda1-and-code-and-ing-buckets" {
  # vars not set here 
  # are default false
  source                               = "./lambda-IAMrole-s3-module/"

  # for first lambda function:
  code_bucket_name                     = "totesys-code-bucket-m1x-7q.r0b"
  lambda_name                          = "extract-lambda"
  runtime                              = "python3.12"
  handler                              = "first_lambda_handler.first_lambda_handler"
  s3_key_for_zipped_lambda             = "1st_lambda_zipped"

  # for lambda exec role policy 
  # that allows lambda to write
  # to ingestion bucket:
  name_of_write_to_bucket              = "totesys-ingestion-bucket-t8v.l5-n6p"

  # Not needed in this call 
  # to the module but you 
  # still have to set it:
  name_of_read_from_bucket              = ""

  # for ingestion bucket:
  # (STILL NEEDED?):
  ing_or_proc_bucket_name              = "totesys-ingestion-bucket-t8v.l5-n6p"

  # conditional vars:
  should_make_s3_code_bucket           = true
  should_make_ing_or_proc_bucket       = true
  should_make_s3_put_obj_policy        = true
  should_make_s3_put_obj_policy_attach = true

                                }



# SECOND LAMBDA
# The 2nd call of the module will provision:
# 1) the 2nd lambda
# 2) the 2nd lambda exec role
# 3) the policy to let a lambda read from a bucket 
# 4) the attachment for 3)
# 5) the policy to let a lambda write to a bucket 
#    (the processed bucket)
# 6) the attachment for 5)
# 7) an s3 bucket (processed) with inline policy 
#    statements that allow the 2nd lambda to write 
#    to it and the 3rd lambda to read from it


module "lambda2-and-proc-bucket" {
  # vars not set here 
  # are default false
  source                               = "./lambda-IAMrole-s3-module"

  # for first lambda function:
  code_bucket_name                     = "totesys-code-bucket-m1x-7q.r0b"
  lambda_name                          = "transform-lambda"
  runtime                              = "python3.12"
  handler                              = "second_lambda_handler.second_lambda_handler"
  s3_key_for_zipped_lambda             = "2nd_lambda_zipped"

  # for lambda exec role policy 
  # that allows lambda to read
  # from ingestion bucket:
  name_of_read_from_bucket             = "totesys-ingestion-bucket-t8v.l5-n6p"


  # for lambda exec role policy 
  # that allows lambda to read
  # from ingestion bucket:
  name_of_write_to_bucket             = "totesys-processed-bucket-h2z-4k.s9w"


  # for processed bucket:
  # (STILL NEEDED?):
  ing_or_proc_bucket_name              = "totesys-processed-bucket-h2z-4k.s9w"

  # conditional vars (value of a var 
  # not set here is false by default):
  should_make_ing_or_proc_bucket       = true
  should_make_s3_put_obj_policy        = true # for put obj into proc bucket
  should_make_s3_put_obj_policy_attach = true
  should_make_s3_get_obj_policy        = true # for get obj from ing bucket
  should_make_s3_get_obj_policy_attach = true

                                }


# The 3rd call of the module will provision:
# 1) the 3rd lambda
# 2) the 3rd lambda exec role
# 3) the policy to let a lambda read from 
#    the processed bucket 
# 4) the attachment for 3)

module "lambda3" {
  # vars not set here are 
  # default false
  source                               = "./lambda-IAMrole-s3-module"

  # for first lambda function:
  code_bucket_name                     = "totesys-code-bucket-m1x-7q.r0b"
  lambda_name                          = "load-lambda"
  runtime                              = "python3.12"
  handler                              = "third_lambda_handler.third_lambda_handler"
  s3_key_for_zipped_lambda             = "3rd_lambda_zipped"

  # Not needed for this call 
  # of the module but you 
  # still need to set it:
  name_of_write_to_bucket             = ""

  # for lambda exec role policy 
  # that allows lambda to read
  # from processed bucket:
  name_of_read_from_bucket             = "totesys-processed-bucket-h2z-4k.s9w"

  # for processed bucket
  # (STILL NEEDED?):
  ing_or_proc_bucket_name              = "totesys-processed-bucket-h2z-4k.s9w"

  # conditional vars (value of a var 
  # not set here is false by default):
  should_make_s3_get_obj_policy        = true # for get obj from proc bucket
  should_make_s3_get_obj_policy_attach = true

                                }
