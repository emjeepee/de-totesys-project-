import json
import boto3
from botocore.exceptions import ClientError


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
    try:
        keys_list = [dict["Key"] for dict in resp_obj.get("Contents", [])]
        # ['design/2025-06-02_22-17-19-2513.json', 'design/2025-05-29_22-17-19-2513.json', etc]
        latest_table_key = sorted(keys_list)[ -1 ]  # 'design/2025-06-02_22-17-19-2513.json'
        response = S3_client.get_object(Bucket=bucket_name, Key=latest_table_key)
        data = response["Body"].read().decode("utf-8")
        return json.loads(data)

    except ClientError as e:
        raise RuntimeError('Error occurred in reading the ingestion bucket') from e

        