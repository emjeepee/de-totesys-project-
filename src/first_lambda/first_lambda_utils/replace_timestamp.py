import logging

from .errors_lookup      import errors_lookup

from botocore.exceptions import ClientError



# __name__ has value "replace_timestamp.py"
logger = logging.getLogger(__name__)




def replace_timestamp(s3_client,
                      bucket_name: str,
                      ts_key: str,
                      now_ts: str
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
            (it is actually always 
            "***timestamp***").

        now_ts: a timestamp string
            for the current time, eg
            "2025-06-04T08:28:12", ie
            no milliseconds            


            
    Returns:
        None            
    
    """


    try:
        # Replace previous timestamp
        # in the bucket with new
        # timestamp:
        s3_client.put_object(Bucket=bucket_name,
                             Key=ts_key,
                             Body=now_ts)

    except ClientError:
        # log the error and 
        # stop the code:
        logger.exception(errors_lookup["err_1"])
        raise