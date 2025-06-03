import boto3
import json

import boto3.exceptions


def read_from_s3(client, bucket_name, key):
    """
    This function reads the given bucket and extracts the data at a given key

    Arguments:
    -The S3 client
    -The name of the bucket to read
    -The key of the file to read

    Returns:
    -Decoded body of the file in the S3
    """
    try:
        response = client.get_object(Bucket=bucket_name, Key=key)
        result = response["Body"].read().decode("utf-8")
        return result
    except boto3.exceptions.Boto3Error as error:
        raise f"Error reading from S3: {error}"


def upload_to_s3(client, bucket_name, key, body):
    """
    This function puts a body into a bucket

    Arguments:
    -The S3 client
    -The name of the bucket upload to
    -The key of the resulting file
    -The body to upload

    Returns:
    -Message indicating success
    """
    try:
        client.put_object(Bucket=bucket_name, Key=key, Body=body)
        return f"Successfully created {key} in {bucket_name}"
    except boto3.exceptions.Boto3Error as error:
        raise f"Error putting to S3: {error}"


def convert_json_to_python(json_data):
    try:
        python_data = json.loads(json_data)
        return python_data
    except Exception as error:
        raise f"Error converting json to python: {error}"


def transform_data():
    pass


def convert_into_parquet():
    pass
