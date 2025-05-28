resource "aws_s3_bucket" "ingestion-bucket" {
  bucket = "11-ingestion-bucket"
  tags = {
    "name" = "ingestion-bucket"
    "environment" = "project"
  }
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket = "11-processed-bucket"
  tags = {
    "name" = "processed-bucket"
    "environment" = "project"
  }
}

resource "aws_s3_bucket" "lambda-bucket" {
  bucket = "11-lambda-bucket"
  tags = {
    "name" = "lambda-bucket"
    "environment" = "project"
  }
}