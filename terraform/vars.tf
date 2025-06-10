variable "first_lambda_function"{
    type = string
    default = "extract_lambda_function"
}

variable "ingestion-bucket"{
    type = string
    default = "11-ingestion-bucket"
}

variable "python_runtime"{
    type = string
    default = "python3.12"
}

variable "aws_region"{
    type = string
    default = "eu-west-2"
}

variable "second_lambda_function"{
    type = string
    default = "second_lambda_function"
}


variable "third_lambda_function"{
    type = string
    default = "third_lambda_function"
}


variable "processed-bucket"{
    type = string
    default = "11-processed-bucket"
}

variable "alert_email_address"{
    type = string
    sensitive = true
    description = "email for alerts"
}


#****************************************************************

# Variables that will contain 
# credentials for accessing
# the ToteSys and warehouse
# databases, and AWS and for
# accessing the alert email 
# address. 
# GitHub Actions (GHA)
# will set these as they are
# specified in workflow.yml:

# For the ToteSys database
#*************************
variable "tf_totesys_db_user" {
#GHA: TF_TOTESYS_DB_USER: "${{ secrets.TF_TOTESYS_DB_USER }}"    
  type      = string
  sensitive = true
  description = "GHA sets this. Holds user string for ToteSys db"
                              }


variable "tf_totesys_db_host" {
# GHA: TF_TOTESYS_DB_HOST: '${{ secrets.TF_TOTESYS_DB_HOST }}'    
  type      = string
  sensitive = true
  description = "GHA sets this. Holds host string for ToteSys db"
}

variable "tf_totesys_db_db" {
# GHA: TF_TOTESYS_DB_DB: '${{ secrets.TF_TOTESYS_DB_DB }}'        
  type      = string
  sensitive = true
  description = "GHA sets this. Holds database string for ToteSys db"
}

variable "tf_totesys_db_password" {
# GHA: TF_TOTESYS_DB_PASSWORD: '${{ secrets.TF_TOTESYS_DB_PASSWORD }}'            
  type      = string
  sensitive = true
  description = "GHA sets this. Holds password string for ToteSys db"
}


variable "tf_totesys_db_port" {
# GHA: TF_TOTESYS_DB_PORT: '${{ secrets.TF_TOTESYS_DB_PORT }}'            
  type      = string
  sensitive = true
  description = "GHA sets this. Holds string for port on which to contact ToteSys db"
}

# For the warehouse database
#***************************
variable "tf_warehouse_db_db" {
# GHA: TF_WAREHOUSE_DB_DB: '${{ secrets.TF_WAREHOUSE_DB_DB }}'            
  type      = string
  sensitive = true
  description = "GHA sets this. Holds database string for warehouse db"
}


variable "tf_warehouse_db_host" {
# GHA: TF_WAREHOUSE_DB_HOST: '${{ secrets.TF_WAREHOUSE_DB_HOST }}'            
  type      = string
  sensitive = true
  description = "GHA sets this. Holds host string for warehouse db"
}


variable "tf_warehouse_db_password" {
# GHA: TF_WAREHOUSE_DB_PASSWORD: '${{ secrets.TF_WAREHOUSE_DB_PASSWORD }}'            
  type      = string
  sensitive = true
  description = "GHA sets this. Holds password string for warehouse db"
}


variable "tf_warehouse_db_port" {
# GHA: TF_WAREHOUSE_DB_PORT: '${{ secrets.TF_WAREHOUSE_DB_PORT }}'            
  type      = string
  sensitive = true
  description = "GHA sets this. Holds string for port on which to contact warehouse db"
}

variable "tf_warehouse_db_user" {
# GHA: TF_WAREHOUSE_DB_USER: '${{ secrets.TF_WAREHOUSE_DB_USER }}'            
  type      = string
  sensitive = true
  description = "GHA sets this. Holds string for user of warehouse db"
}



# For AWS
#*****************
variable "tf_aws_access_key_id" {
#GHA: TF_AWS_ACCESS_KEY_ID: "${{ secrets.TF_AWS_ACCESS_KEY_ID }}"    
  type      = string
  sensitive = true
  description = "GHA sets this. Holds string for AWS access key ID"
}


variable "tf_aws_secret_access_key" {
#GHA:  TF_AWS_SECRET_ACCESS_KEY: '${{ secrets.TF_AWS_SECRET_ACCESS_KEY }}'    
  type      = string
  sensitive = true
  description = "GHA sets this. Holds string for AWS secret access key"
}


# # For the alert email address
# #*****************
# variable "alert_email_address" {
# #GHA: TF_ALERT_EMAIL_ADDRESS: "${{ secrets.TF_ALERT_EMAIL_ADDRESS }}"    
#   type      = string
#   sensitive = true
#   description = "GHA sets this. Holds string alert email address"
# }



