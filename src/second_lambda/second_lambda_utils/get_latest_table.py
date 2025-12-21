import json
import logging

from botocore.exceptions import ClientError

from .errors_lookup import errors_lookup


logger = logging.getLogger(__name__)


def get_latest_table(s3_client, bucket: str, table_name: str):
    """
    This function:
        Gets the latest table of
        the given table name from
        the given S3 bucket.

    Args:
        1) s3_client: a boto3 s3
        client.

        2) bucket: the name of the
        S3 bucket from which to get
        the table.

        3) table_name: the name of
        the table.

    Returns:
        A Python list of
         dictionaries that is the
         latest table of name
         table_name.
    """

    try:
        # Get a list of the
        # objects in the bucket
        # that have the Prefix
        # table_name:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=table_name)
    except ClientError:
        # log the exception
        # and stop the code:
        logger.error(errors_lookup["err_3"])
        raise

    # Make a list of the keys:
    keys_list = [dict["Key"] for dict in resp.get("Contents", [])]
    # keys_list look like:
    # ['design/2025-06-02_22-17-19-2513.json',
    # 'design/2025-05-29_22-17-19-2513.json', etc]

    # Get the key for the latest object:
    latest_table_key = sorted(keys_list)[-1]
    # 'design/2025-06-02_22-17-19-2513.json'

    # Get the table stored under the key latest_table_key:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=latest_table_key)
    except ClientError:
        # log the exception
        # and stop the code:
        logger.error(errors_lookup["err_4"])
        raise

    data = response["Body"].read().decode("utf-8")
    # Convert to Python list and return:
    return json.loads(data)
