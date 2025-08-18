import boto3
import json


from src.first_lambda.first_lambda_utils.update_rows_in_table import update_rows_in_table
from src.first_lambda.first_lambda_utils.get_most_recent_table_data import get_most_recent_table_data
from src.first_lambda.first_lambda_utils.create_formatted_timestamp import create_formatted_timestamp
from src.first_lambda.first_lambda_utils.save_updated_table_to_S3 import save_updated_table_to_S3
from src.first_lambda.first_lambda_utils.update_rows_in_table import update_rows_in_table





# This is the main function:
def write_to_ingestion_bucket(data: list, bucket: str, file_location: str, s3_client: boto3.client):
    """
    This function:
    1. searches the ingestion bucket for all 
        tables that are stored under a key 
        that begins with file_location. This 
        function then gets the most recent of 
        those tables (which is a jsonified 
        lists of dictionaries).
    2. converts the most recent table to a 
        python list of dictionaries.
    3. replaces the appropriate rows in the 
        most recent table.
    4. jsonifies the new updated table.
    5. creates a timestamp string that will be 
        part of the key under which to store the 
        jsonified table and takes this form:
        '2025-05-28_15-45-03'.
    6. stores the new jsonified table in the 
        ingestion bucket under a key that looks
        like 'design/2025-05-28_15-45-03.json',
        where 'design' is the value of arg 
        file_location and '2025-05-28_15-45-03' is 
        the timestamp.
    7. raises a RuntimeError if either of 
        functions get_most_recent_table_data() or 
        save_updated_table_to_S3() raise a 
        RuntimeError.       


    args:
        1) data: a Python list that looks like this:
            [{<data from updated row>}, 
            {<data from updated row>}, etc].
        2) bucket_name: name of the ingestion S3 bucket.
        3) file_location: the name of the table (in the 
            ToteSys database) that has had its rows 
            updated. file_location is also the first 
            part of the key under which this function 
            will store the updated table (the second 
            part being a timestamp for the current time).
            Examples: 'design', 'sales_order', 
            'transactions'.
        4) s3_client: a boto3 S3 client object.            

    returns:
        None
    """

    try:
        # From the bucket get the object that 
        # holds the most recently updated table 
        # whose name is given by file_location.
        # Function get_most_recent_table_data() 
        # could raise a RuntimeError:
        latest_table = get_most_recent_table_data(file_location, s3_client, bucket)
        # a python list of dictionaries.


        # Insert the updated rows into the 
        # retrieved whole table, replacing 
        # the outdated ones:
        updated_table = update_rows_in_table(data, latest_table, file_location)

        # convert updated_table into json:
        updated_table_json = json.dumps(updated_table)

        # Create a formatted timestamp:
        formatted_ts = create_formatted_timestamp()

        # Make a key under which to store the 
        # json list that represents the updated 
        # table:
        new_key = file_location + "/" + formatted_ts + ".json"
        
        # Store the json list that represents 
        # the updated table in the bucket under 
        # the newly created key. Function 
        # save_updated_table_to_S3() could 
        # raise a RuntimeError:
        save_updated_table_to_S3(updated_table_json, s3_client, new_key, bucket)

    except RuntimeError as e:
        raise RuntimeError(str(e)) from e

