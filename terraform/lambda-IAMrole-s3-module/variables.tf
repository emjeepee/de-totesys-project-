# input variables for 
# lambda functions:
# ===================

variable "lambda_name" {
      description = "AWS name of a lambda function"
      type        = string
                       }

variable "code_bucket_name"  {
      description = "AWS name of the code bucket"
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


variable "layer_version_arn" {
      description = "arn of the layer version created in root module"
      type        = string
                             }



variable "lambda_to_trigger" {
      description = "the lambda function that must be triggered by a PUT operation in an S3 bucket"
      
                             }


variable "sns_topic_arn" {
  description = "the arn of the sns topic"

}

# Boolean var to allow selective 
# provisioning of the EventBridge
# scheduler and associated 
# resources:
variable "enable_EvntBrdg_res" {
  type        = bool
  default     = false
  description = "Enable/disable EventBridge-related resources"
  # Only provision the EventBridge 
  # scheduler for the first lambda 
  # function.
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





# for use in the block that
# creates a policy for lambda
# execution role to allow a
# lambda to read from an s3 bucket:
variable "name_of_read_from_bucket"  {
      # value will be either:
      #  1) name of the ingestion bucket 
      #     in the case of the second lambda 
      #  2) name of the processed bucket
      #     in the case of the 3rd lambda.
      description = "name of the bucket from which a lambda will read data."
      type        = string
                   }

# The arn of the bucket that will
# trigger a lambda (used in
# aws_lambda_permission.allow_s3_invoke) 
variable "trigger_bucket_arn" {
  type = string
                              }




# Setting the value of this variable 
# to true means that Terraform 
# provisions an AWS resource-based 
# inline policy that allows the 
# module's lambda to be triggered by
# events from an S3 bucket:
variable "should_make_allow_s3_invoke_policy" {
  description = <<EOT
Used to conditionally provision 
the inline resource-based policy 
that a Lambda needs to allow it 
to be triggered by an event sent 
by an S3 bucket.
EOT
  type = bool
  default = false
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
  description = <<EOT
"When true, code will create 
a policy to allow a lambda 
function to put an object 
in an s3 bucket" 
EOT
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
    description = <<EOT
  "When true, code will provision
   a policy to allow a lambda to 
   get an object from an s3 bucket"
  EOT
                            }


# for conditionally 
# provisioning the notification 
# by the ingestion/processed 
# bucket to the transform/load
# lambda: 
variable "should_make_s3_notif" {
  type    = bool
  default = false
    description = <<EOT
  "When true, code will provision
   an aws_s3_bucket_notification
  EOT
                            }


