# Declare input variable bucket_name.
# Empty curly brackets mean use defaults.
# Pass in actual bucket name when calling 
# the module (eg 
# bucket_name = "totesys-ingestion-bucket-xxxxx").
variable "bucket_name" {
  description = "The name of an S3 bucket"
  type        = string
}

# Provission the resource:
resource "aws_s3_bucket" "mod-buck" {
  bucket = var.bucket_name 
                                    }


# Set two module outputs, one for bucket
# name, the other for the bucket's arn:
output "bucket_name" {
  value = aws_s3_bucket.mod-buck.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.mod-buck.arn
}