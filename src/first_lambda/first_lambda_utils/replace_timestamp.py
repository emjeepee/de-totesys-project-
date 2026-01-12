from botocore.exceptions import ClientError





def replace_timestamp(s3_client,
                      bucket_name: str,
                      ts_key: str,
                      now_ts: str,
                      logger,
                      errors_lookup: dict
                      ):
    
    """
    This function:
        1) tries to replace the 
        previous timestamp in the 
        ingestion bucket with a 
        newer version

        2) logs an exception if 
        the attempt to write to the 
        ingestion bucket fails.


    Args:
        s3_client: a boto3 S3 client
        
        bucket_name: name of the 
            ingestion bucket
        
        ts_key: the key under which 
            the ingestion bucket 
            must store the timestamp

        now_ts: a timestamp string
            for the current time, eg
            "2025-06-04T08:28:12", ie
            no milliseconds            

        logger: a logging object

        errors_lookup: a lookup table
            of error messages

            
    Returns:
        None            
    
    """


    try:
        # Replace previous timestamp
        # in the bucket with new
        # timestamp:
        s3_client.put_object(Bucket=bucket_name, Key=ts_key, Body=now_ts)

    except ClientError:
        # if failure, log the
        # error but allow the
        # code to continue:
        logger.exception(errors_lookup["err_1"]) 