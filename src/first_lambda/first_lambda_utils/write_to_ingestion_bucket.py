from datetime import datetime, timezone
import boto3
import json
from botocore.exceptions import ClientError
import logging
from first_lambda_utils.update_rows_in_table import update_rows_in_table


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
    








def get_latest_table(resp_obj, S3_client: boto3.client, bucket_name: str):
    """
    This function:
        1) Gets the key of the most recent version of a table
            in the S3 ingestion bucket.
        2) Gets the data in the bucket saved under that key
        3) gets called by get_most_recent_table_data()

    Args:
        1) resp_obj: a response object from boto3 method 
            S3_client.list_objects_v2()        
        2) S3_client: a boto3 S3 client
        3) bucket_name: the name of the S3 ingestion bucket
        
    Returns:
        A python list of dictionaries. The list represents 
         a whole table. Each dictionary represents a row 
         of the table.        

    """
    keys_list = [dict["Key"] for dict in resp_obj.get("Contents", [])]
    # ['design/2025-06-02_22-17-19-2513.json', 'design/2025-05-29_22-17-19-2513.json', etc]
    latest_table_key = sorted(keys_list)[ -1 ]  # 'design/2025-06-02_22-17-19-2513.json'
    response = S3_client.get_object(Bucket=bucket_name, Key=latest_table_key)
    data = response["Body"].read().decode("utf-8")
    return json.loads(data)
    




def create_formatted_timestamp():
    """
    This function:
        1) creates a timestamp string formatted like this:
            'YYYY-MM-DD_HH-MM-SS_MS'
    Returns:
        The formatted timestamp string
    Args:
        A timestamp that is a datetime object created by
        the datetime module's now() method
    """
    now_dt_object = datetime.now(timezone.utc)
    formatted_ts = now_dt_object.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_ts






def update_rows_in_table( rows_list: list, json_table_list, file_location: str ):
    
    """
    This function:
        Replaces outdated rows in a passed-in table 
         (which is in the form of a jsonified list) 
         with updated versions held in a passed-in 
         list.
    Args:
        1) rows_list: a python list of dictionaries, each
         dictionary representing a row that contains
         updated data. The number of dictionaries can be 
         from 1 to the number of rows in a whole table.
        2) json_table_list: the table that has to have  
         its outdated rows replaced. This is a jsonified 
         python list of dictionaries, each dictionary
         representing a row. The number of dictionaries
         matches the number of rows of the corresponding
         whole table in the ToteSys database.
        3) file_location: the name of the table. this is
         also the first part of the key under which 
         the S3 bucket stores the table. The second part 
         is a timestamp. The key looks like this,
         for example: 'design/<timestamp-here>' 

    Returns:
        a python list of dictionaries equal in number to
         the number of rows in a particular table in the
         ToteSys database. The list represents an
         updated table. Each dictionary in it represents 
         a row of the table, some of them now updated.
        
    """

    # convert json_table_list to a python list:
    table_list = json.load(json_table_list)

    # file_location is, eg, 'design'.
    # update_row and table_row are dictionaries
    # that include the key 'design_id' (for 
    # example).
    # Find a row in the table and find an
    # updated row where both have the same
    # values for key 'design_id'. Then 
    # replace the table row with the updated 
    # row:
    id_col_name = file_location + "_id"

    new_table_list = [ update_row     
      if update_row[id_col_name] == table_row[id_col_name] else table_row
      for update_row in rows_list    for table_row in table_list    
                     ]

    return new_table_list






def save_updated_table_to_S3(
    updated_table: str, S3_client: boto3.client, new_key: str, bucket: str
):
    """
    This function:
        1. takes the updated table and stores
            it in the S3 bucket
    Args:
        updated_table: a jsonified python list of
            dictionaries.
        S3_client: the boto3 client for S3.
        new_key: a string that is the key under
            which the updated table will be
            saved in the S3 bucket.
        bucket: a string that is the name of the
            S3 bucket
    """
    try:
        S3_client.put_object(Bucket=bucket, Key=new_key, Body=updated_table)
    except ClientError as e:
        return e
