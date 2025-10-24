from botocore.exceptions import ClientError

import logging




logger = logging.getLogger(__name__)


def upload_to_s3(S3_client, bucket_name: str, key: str, body):
    """
    This function:
        puts the given data (body) into the 
        S3 bucket of name bucket_name under the
        key key.

    Args:
        1) S3_client: The boto3 S3 client.
        2) bucket_name: The name of the bucket. 
        3) key: The key under which to store the
            data.
        4) body: The data body to save to the S3 
            bucket.

    Returns:
        None
    """

    err_msg = "Error in upload_to_s3() while trying to write to processed bucket." 

    try:
        S3_client.put_object(Bucket=bucket_name, Key=key, Body=body)
    except ClientError:
        logger.error(err_msg)
        raise 




