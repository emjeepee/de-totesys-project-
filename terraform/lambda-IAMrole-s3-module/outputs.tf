# This output has two uses:
# 1) The first invocation of the 
#    module will make this output 
#    the arn of the ingestion 
#    bucket. The lambda in the 
#    2nd invocation of this 
#    module will employ this 
#    output to set its inline 
#    resource-based policy to it
#    to be triggered by an event 
#    from the ingestion bucket. 
#    The execution role of the lambda 
#    of the second invocation needs
#    this output to set its policy 
#    for reading from the 
#    ingestion bucket.
# 2) The 2nd invocation of the 
#    module will set this output 
#    the arn of the processed 
#    bucket. The lambda in the 
#    3rd invocation of this 
#    module will employ this 
#    output to set its inline 
#    resource-based policy to it
#    to be triggered by an event 
#    from the processed bucket. 
#    The execution role of the lambda 
#    of the 3rd invocation needs
#    this output to set its policy 
#    for reading from the 
#    processed bucket.
output "bucket_that_triggers_next_lambda_arn" {
  value = aws_s3_bucket.mod-ing-or-proc-buck.arn
                                              } 



# There will be two uses of this output:
# 1) the ingestion bucket of the 1st 
#    invocation of the module needs 
#    this output from the 2nd invocation 
#    to set its inline resource-based 
#    policy that allows it to be read 
#    from by the transform lambda.
# 2) the processed bucket of the 2nd 
#    invocation of the module needs 
#    this output from the 3rd invocation 
#    to set its inline resource-based 
#    policy that allows it to be read 
#    from by the load lambda
output "next_lambda_arn" {
  value = aws_lambda_function.mod_lambda.arn
                         } 


