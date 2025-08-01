from datetime import datetime, timezone
import boto3
import json
from botocore.exceptions import ClientError
import logging
from src.first_lambda.first_lambda_utils.update_rows_in_table import update_rows_in_table
from src.first_lambda.first_lambda_utils.get_most_recent_table_data import get_most_recent_table_data
from src.first_lambda.first_lambda_utils.create_formatted_timestamp import create_formatted_timestamp
from src.first_lambda.first_lambda_utils.save_updated_table_to_S3 import save_updated_table_to_S3
from src.first_lambda.first_lambda_utils.update_rows_in_table import update_rows_in_table



logger = logging.getLogger("Mylogger")





# This is the main function:
def write_to_ingestion_bucket(data: dict | list | str, bucket: str, file_location: str, s3_client: boto3.client):
    """
    This function:
    1. searches the ingestion bucket for all jsonified table lists
        that are stored under a key that begins with
        file_location and gets the most recent list.
    2. converts the most recent jsonified table list to a python 
        list (of dictionaries).
    3. replaces the appropriate rows in the python table list.
    4. creates a new jsonified list to represent the updated table
    5. creates a timestamp string that will be part of the key
        under which to store the jsonified list.
    6. stores in the ingestion bucket the new jsonified list that
        represents the updated table. The key for this new json
        list looks like design/2025-05-28_15-45-03.json, where
        'design' is the value of arg file_location and
        '2025-05-28_15-45-03' is a timestamp created in this
        function.


    args:
        1) data: a jsonified version of
            [{<data from one row>}, {<data from one row>}, etc].
            Each dictionaries in the list above represents
            an updated rows of a table. The name of the table is 
            passed in as file_location). 
        2) bucket_name: name of the ingestion S3 bucket.
        3) file_location: this is the name of the table that has been 
            updated in the ToteSys database. It's the first part
            of the key under which this function will store the 
            json list that contains updated rows of a table
            (the second part being a timestamp for the current 
            time).
            Examples: 'design' and 'sales'.

    returns:
        None
    """

    # 1) Read the S3 ingestion bucket for 
    # the object that holds the most recently 
    # updated table data:
    try:
        latest_table = get_most_recent_table_data(file_location, s3_client, bucket)
        # latest_table is a jsonified python list of dictionaries.
        # Convert the passed-in updated rows to python:
        updated_rows = json.loads(data)  # python list like [ {}, {}, {}, etc ]

    except ClientError as e:
        # log error to CloudWatch here
        logger.error("Issue occured while retrieving the most recent table data")
        return e

    try:
        # 3) Insert the updated rows into the retrieved 
        # table.
        # updated_table below is a python list of dictionaries:
        updated_table = update_rows_in_table(updated_rows, latest_table, file_location)

        # convert updated_table into json:
        updated_table_json = json.dumps(updated_table)

    except ClientError as e:
        # log error to CloudWatch here
        logger.error("Ubable to update the S3 bucket with updated rows")

        return e

    try:
        # 4) Create a formatted timestamp:
        formatted_ts = create_formatted_timestamp()

        # 5) Make a string for the key under which to store the json list
        # that represents the updated table. Include the formatted
        # timestamp as part of the key:
        new_key = file_location + "/" + formatted_ts + ".json"

        # 6) Store the json list that represents the updated table into
        # in the ingestion bucket under the newly created key:
        save_updated_table_to_S3(updated_table_json, s3_client, new_key, bucket)
    except ClientError as e:
        # log error to CloudWatch here
        logger.error("Unable to save the data to the s3 bucket")

        return e

    return

