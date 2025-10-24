from botocore.exceptions import ClientError
from datetime import datetime, UTC

import logging

logger = logging.getLogger(__name__)


def change_after_time_timestamp(bucket_name, s3_client, ts_key, default_ts):
    """
    This function:
        1) is the first utility function that 
            the first lambda function calls.
        2) tries to read the timestamp string 
            that the S3 ingestion bucket stores. 
            On success it returns the timestamp.
            On failure this function returns
            the default timestamp (for the year
            1900).
        3) overwrites the existing timestamp 
            string in the bucket with one it
            creates for the current time.
            
    Args:
        1) bucket_name: a string for the name of the
            S3 ingestion bucket.
        2) s3_client: the boto3 S3 client.
        3) ts_key: the key under which the timestamp
            is saved in the bucket (it is actually
            always "***timestamp***").
        4) default_ts: set to "1900-01-01T00-00-00".
            This is the timestamp this function 
            returns either
             i)  on its first ever read of the
                 ingestion bucket or
             ii) if the attempt to get the timestamp
                 from the bucket fails.


    Returns:
        Either --
            1) the previous timestamp if reading the 
                bucket is a success. The first time ever
                that the first lambda function runs this 
                timestamp will be "1900-01-01T00-00-00".
            2) timestamp "1900-01-01T00-00-00" if  
                reading the bucket fails.

    """

    err_msg = "Error in change_after_time_timestamp() during attempt to read ingestion bucket."
    
    # First create a timestamp string for the current time,
    # like this: "2025-06-04T08:28:12":
    now_ts_with_ms = datetime.now(UTC).isoformat()
    # Get rid of the milliseconds:
    now_ts = now_ts_with_ms[:-13]


    try:
        # Get previous timestamp from bucket:
        response = s3_client.get_object(Bucket=bucket_name, Key=ts_key)
        # Replace previous timestamp in the bucket with 
        # new timestamp:
        s3_client.put_object(Bucket=bucket_name, Key=ts_key, Body=now_ts)


    except ClientError: 
        # If there is an error in reading the 
        # S3 ingestion bucket return the 
        # default timestamp (which represents 
        # the year 1900):
        return default_ts


    try:
        # Replace previous timestamp in the bucket with 
        # new timestamp:
        s3_client.put_object(Bucket=bucket_name, Key=ts_key, Body=now_ts)

    except ClientError:
        # If there is an error in writing to
        # the S3 ingestion bucket, log and 
        # propagate the exception:
        logger.error(err_msg)
        logger.info("\n\n\n")
        raise 


    # Return the previous timestamp:
    return response["Body"].read().decode("utf-8")
    
