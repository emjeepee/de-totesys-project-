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

variable "processed-bucket"{
    type = string
    default = "11-processed-bucket"
}

variable "alert_email_address"{
    type = string
    sensitive = true
    description = "email for alerts"
}