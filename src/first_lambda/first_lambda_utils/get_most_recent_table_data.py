from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from src.first_lambda.first_lambda_utils.get_latest_table import get_latest_table

import logging



logger = logging.getLogger("Mylogger")




def get_most_recent_table_data(
    file_location: str, S3_client: boto3.client, bucket_name: str
                              ):
    """
    This function:
        1) gets the latest table of name 
            file_location from the 
            ingestion bucket. The table is
            in the form of a jsonified 
            python list of dictionaries.
            Each dictionary contains data
            from one row of the table.
            The ingestion bucket stores 
            each table under a key that 
            looks like this:
            <file_location>/<timestamp-here>.json.
        2) gets the latest table by getting
            from the ingestion bucket a list 
            of keys whose first part is 
            file_location.
        3) sorts the list of keys according
            to the value of the timestamp,
            descending.
        4) gets the most recent key from the 
            sorted list.
        5) finds the table that the ingestion
            bucket has stored under that most 
            recent key and returns that
            table in the form of a Python 
            list of dictionaries.

    Args:
        1) file_location: part of a key under 
            which the ingestion bucket stores 
            jsonified lists of dictionaries. 
            Each list represents a table and 
            each dictionary in the list 
            represents a row of the table. 
            This arg is also the name of a 
            table, eg 'design'.
        2) S3_client: the boto3 S3 bucket client
        3) bucket_name: the bucket name

    Returns:
        A python list of dictionaries that
         represents the most recently saved 
         table of name file_location.


    """
    try:
        response = S3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_location)
        latest_tbl = get_latest_table(response, S3_client, bucket_name) # a Python list
        return latest_tbl

    except (RuntimeError, ClientError) as e:
        raise RuntimeError from e
