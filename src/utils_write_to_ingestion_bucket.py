from datetime import datetime
import boto3
import json


# /src/utils.py
def write_to_ingestion_bucket(data, bucket, file_location):
    """
    This function:
    1. Searches the ingestion bucket for all jsonified table lists
            that are stored under a key that begins with 
            file_location and gets the most recent list
    2. Converts the most recent list to a python list of dictionaries           
    3. Updates the appropriate rows in the python list
    4. Creates a new json list to represent the updated table
    5. Creates a timestamp string that will be part of the name of 
            the key under which to store the jsonified list.
    5. Stores in the ingestion bucket the new json list that represents 
            the updated table. The key for this new json list looks like 
            design/2025-05-28_15-45-03.json, where 'design' is the value 
            of file_location and '2025-05-28_15-45-03' is a timestamp.  

    args:
        data: a json list of dictionaries, each dictionary representing 
            an updated row of a table in the ToteSys database.
        bucket_name: a string that is the name of the ingestion S3 bucket.
        file_location: a string that is the first part of the key under 
            which this function will store in the ingestion bucket the 
            json list that represents an updated table. Example: 'design'.

    returns:
        A python dictonary with these keys: 
            content -- the value is ??????

    """

    # 1) Get the most recent object that holds a json list that 
    # represents a table: 
    client = boto3.client("s3")
    latest_table = get_most_recent_table_data(file_location, client, bucket)
        # python list like [{}, {}, {}, etc]

    # 2) Create a list of python dictionaries to represent the updated rows of data:
    updated_rows = json.loads(data) # python list like [{}, {}, {}, etc]

    # 3) Insert those updated rows into the retrieved json list of 
    # dictionaries that represents the table:

    # 4) Create a new timestamp and format it:

    # 5) Make a string for the key under which to store the json list
    # that represents the updated table. Include the formatted 
    # timestamp as part of the key:

    # 6) Store the json list that represents the updated table into
    # in the ingestion bucket under the newly created key:


    return 













def get_most_recent_table_data(file_location: str, 
                               S3_client,
                               bucket_name: str
                               ):
    """
    This function:
        1) gets a list of every jsonified list of 
                dictionaries in the bucket if their 
                keys begin with file_location. The
                list will be a python list 
        2) sorts the list according to time, 
                descending
        3) gets the most recent list 
        
    Returns:
        The most recent python list                

    Args:
        file_location: a string that looks like
        'design'
        S3_client: the boto3 S3 bucket client
    bucket_name: a string for the bucket name      
    """
    response = S3_client.list_objects_v2(Bucket=bucket_name, Prefix=file_location)
                # {
                    # 'ResponseMetadata': { ...},
                    #     ...
                    # 'Contents': [
                    #     { 
                    #     'Key': 'design/xxx.json',
                    #     [{...}, {...}, {...}, etc]
                    #     },
                    #     {
                    #     'Key': 'design/yyy.json', 
                    #     ...
                    #     }
                    #             ]
                # }
    keys_list = [dict['Key'] for dict in response.get('Contents', [])]
                # ['design/2025-06-02_22-17-19-2513.json', 'design/2025-05-29_22-17-19-2513.json', etc]
    latest_table_key = sorted(keys_list)[-1] # 'design/2025-06-02_22-17-19-2513.json'
    response = S3_client.get_object(Bucket=bucket_name , Key=latest_table_key)
    data = response['Body'].read().decode("utf-8")
    data_as_py_list = json.loads(data)
    return data_as_py_list
                



    
def create_formatted_timestamp(timestamp):
    """
    This function:
        1) creates a timestamp string of the format
            'YYYY-MM-DD_HH-MM-SS'
    Returns:
        The formatted timestamp string
    Args:
        A timestamp string created by datetime.now(),
        for example: '2025-05-29 22:17:19.251352'
    """
    formatted_ts = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_ts











def update_rows_in_table(rows_list, table_list):
    """
    This function:
        1) updates the appropriate rows in a 
                python list that represesnts a table
    Returns:
        a python list that represents the updated table
    Args:
        rows_list: a python list of dictionaries, each 
        dictionary representing a row that contains
        updated data.
        table_list: a python list of dictionaries that
        represents a table                
    """
    pass













