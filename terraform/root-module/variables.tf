variable "AWS_CODE_BUCKET" {
description = "AWS Lambda service will have env var TF_VAR_AWS_CODE_BUCKET whose value will be the value of this variable. This holds the name of the code bucket"
      type        = string
                       }

variable "AWS_INGEST_BUCKET" {
      description = "The AWS Lambda service will have env var TF_VAR_AWS_INGEST_BUCKET whose value will be the value of this variable.  This holds the name of the ingestion bucket"
      type        = string
                       }


variable "AWS_PROCESS_BUCKET" {
      description = "The AWS Lambda service will have env var TF_VAR_AWS_PROCESS_BUCKET whose value will be the value of this variable.  This holds the name of the processed bucket"
      type        = string
                       }


                       
variable "AWS_ALERT_EMAIL" {
      description = "The AWS Lambda service will have env var TF_VAR_AWS_ALERT_EMAIL whose value will be the value of this variable.  This holds the name of the email to which CloudWatch sends alerts"
      type        = string
                       }
