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
    1. searches the ingestion bucket for all tables
        that are stored under a key that begins with
        file_location. This function then gets the 
        most recent of those tables (remember that 
        the bucket stores each table as a jsonified
        lists of dictionaries).
    2. reasds the most recent table in the ingestion 
        bucket to a python list (of dictionaries).
    3. replaces the appropriate rows in the python 
        table list.
    4. creates a new jsonified list to represent 
        the updated table.
    5. creates a timestamp string that will be 
        part of the key under which to store the 
        jsonified list.
    6. stores the new jsonified list (that 
        represents the updated table) in the 
        ingestion bucket under a key that looks
        like 'design/2025-05-28_15-45-03.json',
        where 'design' is the value of arg 
        file_location and '2025-05-28_15-45-03' is 
        the timestamp that this function created.


    args:
        1) data: a Python list that looks like this:
            [{<data from a row>}, {<data from a row>}, etc].
            Each dictionary in the list represents an
            updated row of a table. The name of the table is 
            passed in as file_location. 
        2) bucket_name: name of the ingestion S3 bucket.
        3) file_location: the name of the table (in the ToteSys
            database) that has had its rows updated. 
            file_location is also the first part of the key 
            under which this function will store the updated
            table (the second part being a timestamp for the
            current time).
            Examples: 'design', 'sales', 'transactions'.

    returns:
        None
    """

    # 1) From the bucket get the object that 
    # holds the most recently updated table 
    # whose name is given by file_location:
    try:
        latest_table = get_most_recent_table_data(file_location, s3_client, bucket)
        # latest_table is a python list of dictionaries.

    except RuntimeError as e:
        raise RuntimeError from e



    # Insert the updated rows into the retrieved 
    # table.
    # updated_table below is a python list of dictionaries.
    # The list represents a whole table, now with updated 
    # rows:
    updated_table = update_rows_in_table(data, latest_table, file_location)

        # convert updated_table into json:
    updated_table_json = json.dumps(updated_table)

    # Create a formatted timestamp:
    formatted_ts = create_formatted_timestamp()

    # Make a string for the key under which to store the json list
    # that represents the updated table. Include the formatted
    # timestamp as part of the key:
    new_key = file_location + "/" + formatted_ts + ".json"
    
    try:
        # Store the json list that represents the updated table in
        # the bucket under the newly created key:
        save_updated_table_to_S3(updated_table_json, s3_client, new_key, bucket)

    except RuntimeError as e:
        raise RuntimeError from e

