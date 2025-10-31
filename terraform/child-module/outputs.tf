# This output has two uses:
# 1) The first invocation of the 
#    module will set this output 
#    to the arn of the ingestion 
#    bucket. The lambda in the 
#    2nd invocation of this 
#    module will employ this 
#    output's value in its inline 
#    resource-based policy that allows it 
#    to be triggered by an event 
#    from the ingestion bucket. 
#    Also the execution role of the 
#    lambda of the second invocation needs
#    this output to set its policy 
#    for reading from the 
#    ingestion bucket.
# 2) The 2nd invocation of the 
#    module will set this output 
#    the arn of the processed 
#    bucket. The lambda in the 
#    3rd invocation of this 
#    module will employ this 
#    output's value to set its inline 
#    resource-based policy that allows
#    it to be triggered by an event 
#    from the processed bucket. 
#    Also the execution role of the lambda 
#    of the 3rd invocation needs
#    this output to set its policy 
#    for reading from the processed bucket.
output "name_of_bckt_that_triggers_next_lambda" {
  value = var.should_make_ing_or_proc_bucket ? aws_s3_bucket.mod-ing-or-proc-buck[0].bucket : null
  # value = aws_s3_bucket.mod-ing-or-proc-buck[0].bucket
                                              } 


# The next module needs this to set 
# the lambda permission that allows
# the lambda to be triggered by the 
# appropriate bucket
output "trigger_bucket_arn" {
  value = var.should_make_ing_or_proc_bucket ? aws_s3_bucket.mod-ing-or-proc-buck[0].arn : null      
                            }


# The lambda function that needs
# to be triggered (by an S3 
# notification in the case of 
# the transform and load lambdas 
# ????and by the EventBridge 
# Scheduler in the case of the 
# extract lambda????):
output "lambda_to_trigger" {
  value = aws_lambda_function.mod_lambda
                            }
# This is tricky!
# Outputs allow one call of a module to 
# refer to a resource in another call of
# the same module.
# In the call to the child module that 
# creates the extract lambda, the ingestion 
# bucket, etc you have to set the value 
# of variable lambda_to_trigger to the 
# transform lambda, which you provision 
# in the second call to the child module.  
# You do it with an output, like this:
# lambda_to_trigger = module.transform.lambda_to_trigger,
#                   ie module.<module call name>.<output name>,
#                   which is how you  refer to outputs.
# whose value will be 
# aws_lambda_function.mod_lambda, where
# 'mod_lambda' is the terraform name of
# the lambda that you specify in the 
# child module 'template' in these lines:
# resource "aws_lambda_function" "mod_lambda" {
#  function_name = var.lambda_name
#  role          = aws_iam_role.lambda_exec.arn
# etc


