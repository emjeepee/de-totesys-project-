


# a lookup table for 
# error messages that 
# first lambda utlilities 
# will employ when logging 

 
errors_lookup = {
'err_0': "Error caught in read_from_s3() while trying to read ingestion bucket.",    
"err_1": "Error caught in is_first_run_of_pipeline() while trying to read the ingestion bucket.",
"err_2": "Error caught in upload_to_s3() while trying to write to the processed bucket.",    
"err_3": "Error caught in get_latest_table() while trying to read the ingestion bucket."
                }


