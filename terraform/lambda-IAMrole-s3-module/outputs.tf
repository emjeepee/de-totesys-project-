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
  # value = aws_s3_bucket.mod-ing-or-proc-buck.bucket
  value = var.ing_or_proc_bucket_name
                                              } 




