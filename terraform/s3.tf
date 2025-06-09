resource "aws_s3_bucket" "ingestion-bucket" {
  bucket = "11-ingestion-bucket"
  tags = {
    "name" = "ingestion-bucket"
    "environment" = "project"
  }
}

resource "aws_s3_bucket_versioning" "ingestion-versioning" {
  bucket = aws_s3_bucket.ingestion-bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "processed-bucket" {
  bucket = "11-processed-bucket"
  tags = {
    "name" = "processed-bucket"
    "environment" = "project"
  }
}

resource "aws_s3_bucket_versioning" "processed_versioning" {
  bucket = aws_s3_bucket.processed-bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}


# this is the bucket that will contain each
# zipped lambda file code for each lambda
# function:
resource "aws_s3_bucket" "lambda-bucket" {
  bucket = "11-lambda-bucket"
  tags = {
    "name" = "lambda-bucket"
    "environment" = "project"
  }
}

resource "aws_s3_bucket_versioning" "lambda_versioning" {
  bucket = aws_s3_bucket.lambda-bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}