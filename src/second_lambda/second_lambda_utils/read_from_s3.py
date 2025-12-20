from botocore.exceptions import ClientError

import logging

from .errors_lookup import errors_lookup


logger = logging.getLogger(__name__)


def read_from_s3(s3_client, bucket_name: str, key: str):
    """
    This function:
        looks for the object that the given bucket stores
        under the given key and extracts the data in that
        object.

    Arguments:
        1) A boto3 S3 client.
        2) The name of the bucket.
        3) The key.

    Returns:
        The decoded body of the file stored in the
        the given S3 bucket under the given key.
    """

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        result = response["Body"].read().decode("utf-8")
        return result

    except ClientError:
        # log the error
        # and stop the code:
        logger.exception(errors_lookup["err_0"])
        raise
