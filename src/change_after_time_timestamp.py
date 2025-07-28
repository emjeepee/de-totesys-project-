from src.utils_write_to_ingestion_bucket import create_formatted_timestamp

import botocore
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, UTC


def change_after_time_timestamp(bucket_name, s3_client, ts_key, default_ts):
    """
    This function:
        1) is the first utility function that 
            the first lambda function calls.
        2) reads the timestamp string in the 
            S3 ingestion bucket and returns it.
        3) overwrites the existing timestamp 
            string in the S3 bucket with a 
            a timestamp string that represents
            the current time.
        4) returns the existing timestamp string
            if reading the S3 ingestion bucket
            is successful.
        5) returns the default timestamp string
            if reading the S3 ingestion bucket
            is not successful.
            
    Args:
        1) bucket_name: a string for the name of the
            S3 ingestion bucket.
        2) s3_client: the boto3 S3 client.
        3) ts_key: the key under which the timstamp
            is saved in the bucket (it is actually
            always "***timestamp***").
        4) default_ts: a timestamp this function 
            returns on its very first read of the
            ingestion bucket (ie "1900-01-01-00-00-00")


    Returns:
        Either --
            1) the saved timestamp if reading the 
                bucket is a success. The first time ever
                that the lambda function runs this 
                timestamp will be "1900-01-01-00-00-00".
            2) timestamp "1900-01-01-00-00-00" if  
                reading the bucket fails.

    """

    
    # First create a timestamp string for the current time,
    # like this: "2025-06-04T08:28:12":
    now_ts_with_ms = datetime.now(UTC).isoformat()
    now_ts = now_ts_with_ms[:-13]


    try:
        # Get previous timestamp from bucket:
        response = s3_client.get_object(Bucket=bucket_name, Key=ts_key)
        
        # Replace previous timestamp with 
        # new timestamp:
        s3_client.put_object(Bucket=bucket_name, Key=ts_key, Body=now_ts)

        # Return the previous timestamp:
        return response["Body"].read().decode("utf-8")
    
    except ClientError:
        # If there is an error in reading the 
        # S3 ingestion bucket return the 
        # default timestamp (which represents 
        # the year 1900):
        return default_ts
