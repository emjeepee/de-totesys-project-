"""
Sudo code:
Create a function to:
    take a given json data
    write it to the S3 ingestion bucket

"""

import boto3
import json
# /src/utils.py
def write_to_ingestion_bucket(data, bucket, file_location):
    """
    This fucntion: 
    1. Get the appropriate object from the ingestion bucket
    2. Change the appropriate rows in the table respresented by the object
    3. Put the object in the ingestion bucket, naming it with a timestamp in its name


    args:
        data: a json string to be added to object_key
        bucket_name: name of the target S3 bucket
        object_key: file name to be saved as in the ingestion bucket.
    returns:
        none ? name of the key it saved in the bucket 

    """
    client = boto3.client("s3")
    file = client.get_object(Bucket=bucket, Key=file_location)
    content = file["Body"].read().decode("utf-8")
    file1 = client.get_object(Bucket=bucket, Key=file_location)
    return {"content": content}