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

# The first call of the module will provision:
# 1) the first lambda
# 2) the first lambda exec role
# 3) the policy to let a lambda write to a bucket 
# 4) the attachment for the policy
# 5) an s3 bucket (ingestion) with inline policy 
#    statements that allow the first lambda to 
#    write to it and the second lambda to read 
#    from it
# 6) The code bucket


# The 2nd call of the module will provision:
# 1) the 2nd lambda
# 2) the 2nd lambda exec role
# 3) the policy to let a lambda read from a bucket 
# 4) the attachment for 3)
# 5) the policy to let a lambda write to a bucket 
#    (the processed bucket)
# 6) the attachment for 5)
# 7) an s3 bucket (processed) with inline policy 
#    statements that allow the 2nd lambda to write 
#    to it and the 3rd lambda to read from it


# The 3rd call of the module will provision:
# 1) the 3rd lambda
# 2) the 3rd lambda exec role
# 3) the policy to let a lambda read from 
#    the processed bucket 
# 4) the attachment for 3)







# Provision an IAM execution role for 
# a lambda function. (NOTE: "lambda_exec"
# cannot be dynamic, ie you cannot include 
# a variable value in that string!! 
# Instead, if you need several roles just 
# like this one, use count of for each):
resource "aws_iam_role" "lambda_exec" {
  name = "${var.lambda_name}-IAM-role"

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




# Provision a lambda function:
resource "aws_lambda_function" "mod_lambda" {
  function_name = var.lambda_name
  role          = aws_iam_role.lambda_exec.arn
  runtime       = var.runtime
  handler       = var.handler
  s3_bucket     = var.bucket_name # will be code bucket 
  s3_key        = var.s3_key_for_func

  environment {
#     variables = var.environment_vars
              }
                                            }


# 
# should_make_s3_get_obj_policy


# Provision policy for 
# the execution role to 
# allow a lambda to write 
# to a bucket:
resource "aws_iam_policy" "lambda_policy" {
  count = var.should_make_s3_put_obj_policy ? 1 : 0

  name   = "${var.lambda_name}-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:PutObject"],
      Resource = "arn:aws:s3:::${var.bucket_name}/*"
    }]
  })
}



# Provision policy attachment
# for a lambda function
resource "aws_iam_role_policy_attachment" "lambda_attach" {
  count      = var.should_make_s3_put_obj_policy_attach ? 1 : 0
  role       = aws_iam_role.lambda_exec[0].name
  policy_arn = aws_iam_policy.lambda_policy[0].arn
                                                          }






# Provision an s3 bucket:
resource "aws_s3_bucket" "mod-buck" {
  count  = var.should_make_bucket ? 1 : 0
  bucket = var.bucket_name 
                                    }
