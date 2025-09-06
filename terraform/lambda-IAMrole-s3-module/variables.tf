# input variables for 
# lambda functions:
# ===================

variable "lambda_name" {
      description = "AWS name of a lambda function"
      type        = string
                       }

variable "code_bucket_name"  {
      description = "AWS name of ingestion/processed/code bucket"
      type        = string
                          }


variable "s3_key_for_zipped_lambda"  {
      description = "key under which to store zipped lambda in code bucket"
      type        = string
                       }


variable "runtime"  {
      description = "version of Python"
      type        = string
                       }


variable "handler"  {
      # format is <filename>.<function_name>
      description = "name of Python function that is lambda handler"
      type        = string
                   }



# for use in the block that
# creates a policy for lambda
# execution role to allow a
# lambda to write to an s3 bucket:
variable "name_of_write_to_bucket"  {
      # value will be either:
      #  1) name of the ingestion bucket 
      #     in the case of the first lambda 
      #  2) name of the processed bucket
      #     in the case of the 2nd lambda.
      description = "name of the bucket to which a lambda will write data."
      type        = string
                   }








# for s3 buckets
# ==============

# for name of ingestion
# or processed bucket:
variable "ing_or_proc_bucket_name"  {
      description = "AWS name of ingestion/processed bucket"
      type        = string
                                    }

# var for name of code bucket is above.





# CONDITIONAL VARIABLES
# =====================

# for conditionally 
# provisioning ingestion 
# bucket or processed 
# bucket: 
variable "should_make_ing_or_proc_bucket" {
  type    = bool
  default = false
  description = "when true, code will create bucket"
                              }


# for conditionally 
# provisioning the 
# s3 code bucket: 
variable "should_make_s3_code_bucket" {
  type    = bool
  default = false
  description = "when true, code will create the s3 code bucket"
                                      }



# for conditionally 
# provisioning an identity-based
# policy to attach to a lambda's 
# execution role to allow the 
# lambda to put an object in an 
# s3 bucket: 
variable "should_make_s3_put_obj_policy" {
  type    = bool
  default = false
  description = "when true, code will create a policy to allow a lambda function to put an object in an s3 bucket"
                                         }


# for conditionally 
# provisioning an attachment to 
# to attach the put object policy 
# to a lambda's execution role: 
variable "should_make_s3_put_obj_policy_attach" {
  type    = bool
  default = false
  description = "when true, code will create a policy attachment to allow a policy that allows a lambda function to write to a bucket be attached to the lambda's execution role"
                            }


# for conditionally 
# provisioning an identity-based
# policy to attach to a lambda's 
# execution role to allow the 
# lambda to get an object from 
# an s3 bucket: 
variable "should_make_s3_get_obj_policy" {
  type    = bool
  default = false
  description = "when true, code will create a policy to allow a lambda function to get an object from an s3 bucket"
                            }



# for conditionally 
# provisioning an attachment to 
# to attach the get object policy 
# to a lambda's execution role: 
variable "should_make_s3_get_obj_policy_attach" {
  type    = bool
  default = false
  description = "when true, code will create a policy attachment to allow a policy that allows a lambda function to read from a bucket be attached to the lambda's execution role"
                                                }

