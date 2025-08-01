from datetime import datetime, timezone
import boto3
import json
from botocore.exceptions import ClientError
from src.first_lambda.first_lambda_utils.get_latest_table import get_latest_table

import logging



logger = logging.getLogger("Mylogger")




def get_most_recent_table_data(
    file_location: str, S3_client: boto3.client, bucket_name: str
                              ):
    """
    This function:
        1) gets a list of every jsonified dictionary
            in the bucket if their
            keys begin with file_location. The
            list will be a python list
        2) sorts the list according to time,
            descending
        3) gets the most recent list

    Args:
        1) file_location: the name of a table, eg
            'design'
        2) S3_client: the boto3 S3 bucket client
        3) bucket_name: the bucket name

    Returns:
        The most recent python dictionary


    """
    try:
        response = S3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_location)
    except ClientError as e:
        return e

    try:
        return get_latest_table(response, S3_client, bucket_name)

    except ClientError as e:
        return e
