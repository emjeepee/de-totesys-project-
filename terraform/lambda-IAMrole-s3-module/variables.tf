# define input variables:
variable "lambda_name" {
      description = "name of a lambda function in AWS"
      type        = string
                       }
variable "s3_bucket_name"  {
      description = "name of ingestion/processed/code bucket in AWS"
      type        = string
                       }
variable "s3_function_key"  {
      description = "key under which to store a zipped lambda function in the code s3 bucket"
      type        = string
                       }
variable "runtime"  {
      description = "version of Python"
      type        = string
                       }
variable "handler"  {
      description = "name of the Python function that is the actual handler for the lambda function"
      type        = string
                   }

# Next two are for conditionally 
# provisioning 
# 
variable "should_make_bucket" {
  type    = bool
  default = true
  description = "when set to true, code will create the bucket"
                              }

variable "should_make_role" {
  type    = bool
  default = true
  description = "when set to true, code will create the role"
                            }



