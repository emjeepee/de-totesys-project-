import logging

from botocore.exceptions import ClientError
from .errors_lookup      import errors_lookup

# __name__ has value "get_timestamp.py"
logger = logging.getLogger(__name__)



def get_timestamp(s3_client, 
                  bucket_name: str,
                  ts_key: str,
                  default_ts: str
                  ):
    
    """
    This function:
        1) tries to get the previous 
        timestamp from the 
        ingestion bucket and 
        returns it if successful.

        2) returns the default 
        timestamp if attempt to get
        it from the ingestion bucket 
        fails.


    Args:
        s3_client: a boto3 S3 client
        
        bucket_name: name of the 
            ingestion bucet
        
        ts_key: the key under which 
            the ingestion bucket 
            has stored the timestamp

        errors_lookup: a lookup table
            of error messages

        default_ts: the default 
            timestamp for the year 
            1900.

    Returns:
        either the previously stored 
        timestamp string or the default 
        timestamp string for the year 
        1900.


    """    


    try:
        # Get previous timestamp
        # from bucket:
        response = s3_client.get_object(Bucket=bucket_name, 
                                        Key=ts_key)
        return response["Body"].read().decode("utf-8")

    except ClientError:
        # boto3 raises this
        # exception either on the
        # first ever run of the
        # pipeline (because the
        # s3 bucket contains no
        # timestamp) or if reading 
        # the ingestion bucket 
        # fails.
        # Return the timestamp
        # for the year 1900:
        logger.exception(errors_lookup["err_0"])   
        return default_ts