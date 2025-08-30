# define input variables:
variable "lambda_name" {
      description = "name of a lambda function in AWS"
      type        = string
                       }

variable "bucket_name"  {
      description = "name of ingestion/processed/code bucket in AWS"
      type        = string
                          }

variable "s3_key_for_func"  {
      description = "key under which to store a zipped lambda function in the code s3 bucket"
      type        = string
                       }

variable "runtime"  {
      description = "version of Python"
      type        = string
                       }

variable "handler"  {
      description = "name of the Python function that is the lambda handler"
      type        = string
                   }



# Var for conditionally 
# provisioning the ingestion 
# bucket or the processed 
# bucket: 
variable "should_make_bucket" {
  type    = bool
  default = false
  description = "when true, code will create bucket"
                              }


# Var for conditionally 
# provisioning the s3 code bucket: 
variable "should_make_s3_code_bucket" {
  type    = bool
  default = false
  description = "when true, code will create the s3 code bucket"
                                      }



# Var for conditionally 
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


# Var for conditionally 
# provisioning an attachment to 
# to attach the put object policy 
# to a lambda's execution role: 
variable "should_make_s3_put_obj_policy_attach" {
  type    = bool
  default = false
  description = "when true, code will create a policy attachment to allow a policy that allows a lambda function to write to a bucket be attached to the lambda's execution role"
                            }


# Var for conditionally 
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



# Var for conditionally 
# provisioning an attachment to 
# to attach the get object policy 
# to a lambda's execution role: 
variable "should_make_s3_get_obj_policy_attach" {
  type    = bool
  default = false
  description = "when true, code will create a policy attachment to allow a policy that allows a lambda function to read from a bucket be attached to the lambda's execution role"
                                                }





