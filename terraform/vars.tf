variable "first_lambda_function"{
    type = string
    default = "first_lambda_function"
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