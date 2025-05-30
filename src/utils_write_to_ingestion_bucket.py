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
            json list that represents an updated table. It is also the
            name of the table that has been changed in the ToteSys
            database. Example: 'design'.

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
    updated_rows = json.loads(data)  # python list like [ {}, {}, {}, etc ]

    # 3) Insert those updated rows into the retrieved json list of
    # dictionaries that represents the table
    # updated_table below is a python list of dictionaries:
    updated_table = update_rows_in_table(updated_rows, latest_table, file_location)

    updates_table_json = json.dumps(updated_table)

    # 4) Create a formatted timestamp:
    formatted_ts = create_formatted_timestamp()

    # 5) Make a string for the key under which to store the json list
    # that represents the updated table. Include the formatted
    # timestamp as part of the key:
    new_key = file_location + "/" + formatted_ts + ".json"

    # 6) Store the json list that represents the updated table into
    # in the ingestion bucket under the newly created key:
    # store updated_table in s3 bucket under key new_key
    save_updated_table_to_S3(updates_table_json, client, new_key, bucket)

    return


def get_most_recent_table_data(file_location: str, S3_client, bucket_name: str):
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
    keys_list = [dict["Key"] for dict in response.get("Contents", [])]
    # ['design/2025-06-02_22-17-19-2513.json', 'design/2025-05-29_22-17-19-2513.json', etc]
    latest_table_key = sorted(keys_list)[-1]  # 'design/2025-06-02_22-17-19-2513.json'
    response = S3_client.get_object(Bucket=bucket_name, Key=latest_table_key)
    data = response["Body"].read().decode("utf-8")
    data_as_py_list = json.loads(data)
    return data_as_py_list


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
    now_dt_object = datetime.now()
    formatted_ts = now_dt_object.strftime("%Y-%m-%d_%H-%M-%S")
    return formatted_ts


def update_rows_in_table(rows_list: list, table_list: list, file_location: str):
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

    # file_location is eg 'design'
    key_to_search = file_location + "_id"

    for dct in rows_list:
        for row_dct in table_list:
            if dct[key_to_search] == row_dct[key_to_search]:
                # dict['design_id'] is a dictionary
                # table_list['design_id'] is a dictionary

                # replace the row in table_list
                ind = table_list.index(row_dct)
                table_list[ind] = dct
    return table_list


def save_updated_table_to_S3(updated_table, S3_client, new_key, bucket):
    """
    This function:
        1. takes the updated table and stores
            it in the S3 bucket
    Args:
        updated_table: a jsonified python list of
            dictionaries.
        S3_client is the boto3 client for S3.
        new_key: a string that is the key under
            which the updated table will be
            saved in the S3 bucket.
        bucket: a string that is the name of the
            S3 bucket
    """
    S3_client.put_object(Bucket=bucket, Key=new_key, Body=updated_table)
