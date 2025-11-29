import logging

from botocore.exceptions import ClientError
from datetime import datetime, UTC

from .errors_lookup import errors_lookup




logger = logging.getLogger(__name__)


def change_after_time_timestamp(bucket_name, s3_client, ts_key, default_ts):
    """
    This function:
        1) is the first utility 
            function that the first 
            lambda handler calls.
        2) tries to read the 
            timestamp string that 
            the S3 ingestion bucket 
            stores.
            On success it returns 
            the timestamp. 
            On failure it returns
            the default timestamp, 
            which is for the year 
            1900.
        3) overwrites the existing 
            timestamp string in the 
            bucket with one it 
            creates for the current 
            time.
            
    Args:
        1) bucket_name: a string 
            for the name of the S3 
            ingestion bucket.

        2) s3_client: the boto3 S3 
            client.

        3) ts_key: the key under which 
            the timestamp is saved in 
            the bucket (it is actually
            always "***timestamp***").

        4) default_ts: set to 
            "1900-01-01T00-00-00".
            This is the timestamp this 
            function returns either
             i)  on its first ever 
                 read of the
                 ingestion bucket or
             ii) if the attempt to get 
                 the timestamp from the 
                 bucket fails.


    Returns:
        Either --
            1) the previous timestamp 
                if reading the bucket is 
                a success. 
            2) "1900-01-01T00-00-00" if 
                it's the first time ever 
                that the pipeline has run.
            3) "1900-01-01T00-00-00" if  
                this is the 2nd-plus time 
                the pipeline has run but 
                reading the ingestion 
                bucket for the previous 
                timestamp has failed.

    """

    
    # create a timestamp string 
    # for the current time, eg
    # "2025-06-04T08:28:12", ie 
    # no milliseconds:
    now_ts = datetime.now(UTC).isoformat()[:-13]

    try:
        # Get previous timestamp 
        # from bucket:
        response = s3_client.get_object(
            Bucket=bucket_name, 
            Key=ts_key)

    except ClientError: 
        # if failure, return
        # timestamp for the 
        # year 1900:    
        logger.error(errors_lookup['err_0'])
        return default_ts
    

    try:        
        # Replace previous timestamp 
        # in the bucket with new 
        # timestamp:
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=ts_key, 
            Body=now_ts)

    except ClientError: 
        # if failure, log the 
        # error but allow the 
        # code to continue:
        logger.error(errors_lookup['err_1'])
        

    try:
        # Replace previous timestamp 
        # in the bucket with the
        # new timestamp:
        s3_client.put_object(
            Bucket=bucket_name, 
            Key=ts_key, 
            Body=now_ts)

    except ClientError:
        # If failure, log and 
        # stop the code:
        logger.error(errors_lookup['err_2'])
        raise 



    # Return the previous 
    # timestamp:
    return response["Body"].read().decode("utf-8")

    
