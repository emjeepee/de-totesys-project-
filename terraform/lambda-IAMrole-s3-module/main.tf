# What needs to be provisioned:
# 1) A lambda function
# 2) An execution role for the lambda function
# 3) a standalone policy to let a lambda function
#    write to a bucket  
# 4) a policy attachment for the policy in 3)
# 5) a standalone policy to let a lambda function
#    read from a bucket  
# 6) a policy attachment for the policy in 5)
# 7) an s3 bucket with inline policy that 
#    has two statements:
#    i)   one that allows a lambda to write 
#         data to it  
#    ii)  one that allows a lambda to read 
#         data from it  
# 8) an s3 code bucket with inline policy that 
#    ???:
#    ???




# LOAD LAYER ZIP AND LAMBDA FUNCTIONS' 
# ZIPS INTO THE CODE BUCKET
#=========================================

# put the layer zip file object 
# in the code bucket: 
resource "aws_s3_object" "layer_zip" {
  bucket = "totesys-code-bucket-m1x-7q.r0b"
  key    = "zipped/layer.zip"
  source = "../zipped_files/layer.zip"
}

# put the objects that are the 
# lambda zip files in the code 
# bucket: 
resource "aws_s3_object" "first_lambda_zip" {
  bucket = "totesys-code-bucket-m1x-7q.r0b"
  key    = "zipped/first_lambda.zip"
  source = "../zipped_files/first_lambda.zip" # must be relative to terraform dir
}

resource "aws_s3_object" "second_lambda_zip" {
  bucket = "totesys-code-bucket-m1x-7q.r0b"
  key    = "zipped/second_lambda.zip"
  source = "../zipped_files/second_lambda.zip"  # must be relative to terraform dir
}

resource "aws_s3_object" "third_lambda_zip" {
  bucket = "totesys-code-bucket-m1x-7q.r0b"
  key    = "zipped/third_lambda.zip"
  source = "../zipped_files/third_lambda.zip"  # must be relative to terraform dir
}





# CREATE LAMBDA LAYER VERSION
# (SHARED BY THE THREE LAMBDAS)
# =============================
resource "aws_lambda_layer_version" "shared-layer" {
  layer_name          = "layer-shared-by_all_lambdas"
  s3_bucket           = "totesys-code-bucket-m1x-7q.r0b"
  s3_key              = aws_s3_object.layer_zip.key
  compatible_runtimes = ["python3.13"]
}




# A LAMBDA FUNCTION AND ITS
# EXECUTION ROLE
# =========================

# Provision a lambda function:
resource "aws_lambda_function" "mod_lambda" {
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_exec.arn
  runtime       = var.runtime
  handler       = var.handler # format is <filename>.<function_name>
  s3_bucket     = var.code_bucket_name # will be code bucket 
  s3_key        = var.s3_key_for_zipped_lambda # site of zipped code


  layers = [
    aws_lambda_layer_version.shared-layer.arn
           ]

  environment {
#     variables = var.environment_vars
              }
                                            }

# Provision an IAM execution role for 
# a lambda function. (NOTE: "lambda_exec"
# cannot be dynamic, ie you cannot include 
# a variable value in that string!! 
# Instead, if you need several roles just 
# like this one, use count of for each):
resource "aws_iam_role" "lambda_exec" {
  name = "${var.lambda_name}-IAM-role"
# "lambda_exec" is the Terraform name
# "${var.lambda_name}-IAM-role" is the aws name

# Define the trust policy to allow 
# lambda functions to assume this 
# role (could also be standalone 
# instead of inline):
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}





# POLICY AND ATTACHMENT TO LET
# A LAMBDA WRITE TO AN S3
# =============================

# Provision the policy for 
# a lambda's execution role to 
# allow the lambda to write 
# to a bucket:
resource "aws_iam_policy" "lambda_put_policy" {
  count = var.should_make_s3_put_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:PutObject"],
      Resource = "arn:aws:s3:::${var.name_of_write_to_bucket}/*"
    }]
  })
}


# Provision policy attachment
# to attach s3 put policy to
# lambda execution role:
resource "aws_iam_role_policy_attachment" "lambda_put_attach" {
  count      = var.should_make_s3_put_obj_policy_attach ? 1 : 0
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_put_policy[0].arn
                                                          }


# POLICY AND ATTACHMENT TO LET
# A LAMBDA READ FROM AN S3
# =============================

# Provision the policy for 
# a lambda execution role to 
# allow the lambda to read
# from a bucket:
resource "aws_iam_policy" "lambda_get_policy" {
  count = var.should_make_s3_get_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:GetObject"],
      Resource = "arn:aws:s3:::${var.name_of_read_from_bucket}/*"
    }]
  })
}


# Provision policy attachment
# for the policy above
resource "aws_iam_role_policy_attachment" "lambda_get_attach" {
  count      = var.should_make_s3_get_obj_policy_attach ? 1 : 0
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_get_policy[0].arn
                                                          }





# THE S3 BUCKETS
# ==============

# Provision either the ingestion or 
# the processed bucket:
resource "aws_s3_bucket" "mod-ing-or-proc-buck" {
  count  = var.should_make_ing_or_proc_bucket ? 1 : 0
  bucket = var.ing_or_proc_bucket_name
                                    }



# Provision the code bucket:
resource "aws_s3_bucket" "mod-code-buck" {
  count  = var.should_make_s3_code_bucket ? 1 : 0
  bucket = var.code_bucket_name
                                        }

