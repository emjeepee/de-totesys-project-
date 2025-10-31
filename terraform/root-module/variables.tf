variable "AWS_INGEST_BUCKET" {
      description = "The AWS Lambda service will have env var TF_VAR_AWS_INGEST_BUCKET whose value will be the value of this variable.  This holds the name of the ingestion bucket"
      type        = string
                       }

variable "AWS_PROCESS_BUCKET" {
      description = "The AWS Lambda service will have env var TF_VAR_AWS_PROCESS_BUCKET whose value will be the value of this variable.  This holds the name of the processed bucket"
      type        = string
                       }

variable "AWS_CODE_BUCKET" {
description = "AWS Lambda service will have env var TF_VAR_AWS_CODE_BUCKET whose value will be the value of this variable. This holds the name of the code bucket"
      type        = string
                       }

                       
variable "AWS_ALERT_EMAIL" {
      description = "The AWS Lambda service will have env var TF_VAR_AWS_ALERT_EMAIL whose value will be the value of this variable.  This holds the name of the email to which CloudWatch sends alerts"
      type        = string
                       }

variable "AWS_TABLES_LIST" {
      description = "The AWS Lambda service will have env var TF_VAR_AWS_TABLES_LIST whose value will be the value of this variable. This holds a list of the names of all the tables this project deals with"
      type        = string
                       }

variable "TOTE_SYS_DB_USER" {
      description = "The AWS Lambda service will have env var TF_VAR_TOTE_SYS_DB_USER whose value will be the value of this variable. This holds the username of the ToteSys database"
      type        = string
                       }

variable "TOTE_SYS_DB_PASSWORD" {
      description = "The AWS Lambda service will have env var TF_VAR_TOTE_SYS_DB_PASSWORD whose value will be the value of this variable. This holds the password of the ToteSys database"
      type        = string
                       }

variable "TOTE_SYS_DB_DB" {
      description = "The AWS Lambda service will have env var TF_VAR_TOTE_SYS_DB_DB whose value will be the value of this variable. This holds the database of the ToteSys database"
      type        = string
                       }

variable "TOTE_SYS_DB_PORT" {
      description = "The AWS Lambda service will have env var TF_VAR_TOTE_SYS_DB_PORT whose value will be the value of this variable. This holds the port number of the ToteSys database"
      type        = string
                       }

variable "TOTE_SYS_DB_HOST" {
      description = "The AWS Lambda service will have env var TF_VAR_TOTE_SYS_DB_HOST whose value will be the value of this variable. This holds the host address of the ToteSys database"
      type        = string
                       }

variable "WAREHOUSE_DB_USER" {
      description = "The AWS Lambda service will have env var TF_VAR_WAREHOUSE_DB_USER whose value will be the value of this variable. This holds the username of the warehouse database"
      type        = string
                       }
variable "WAREHOUSE_DB_PASSWORD" {
      description = "The AWS Lambda service will have env var TF_VAR_WAREHOUSE_DB_PASSWORD whose value will be the value of this variable. This holds the password of the warehouse database"
      type        = string
                       }

variable "WAREHOUSE_DB_DB" {
      description = "The AWS Lambda service will have env var TF_VAR_WAREHOUSE_DB_DB whose value will be the value of this variable. This holds the database name of the warehouse database"
      type        = string
                       }

variable "WAREHOUSE_DB_PORT" {
      description = "The AWS Lambda service will have env var TF_VAR_WAREHOUSE_DB_PORT whose value will be the value of this variable. This holds the port of the warehouse database"
      type        = string
                       }

variable "WAREHOUSE_DB_HOST" {
      description = "The AWS Lambda service will have env var TF_VAR_WAREHOUSE_DB_HOST whose value will be the value of this variable. This holds the host address of the warehouse database"
      type        = string
                       }
