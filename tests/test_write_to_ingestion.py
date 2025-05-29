from moto import mock_aws
import boto3
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime
import os
from src.utils_write_to_ingestion_bucket import write_to_ingestion_bucket

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

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="module")
def mock_S3_client():
    with mock_aws():
        yield boto3.client('s3',region_name="eu-west-2")

# Create mock data
timestamp = datetime.now()
bucket_name = "11-ingestion-bucket"
object_key = f"ingested/design-{timestamp}.json"

# Create mock bucket

# Create object with mock data
body = '{"name": "abdul", "team": 11, "project": "terraform"}'


# mock data 
body_1 = '{"name": "Neil", "team": 12, "project": "writing JSON"}'
mock_table = {"design": [{"name": "abdul", "team": 11, "project": "terraform"},
                             {"name": "Mukund", "team": 11, "project": "terraform"},
                             {"name": "Neil", "team": 11, "project": "terraform"}]
                             }
expected_table = [
        {"name": "abdul", "team": 11, "project": "terraform"},
        {"name": "Mukund", "team": 11, "project": "terraform"},
        {"name": "Neil", "team": 12, "project": "writing JSON"}
        ]
                             
mock_json = json.load(mock_table)


def test_it_can_read_object_from_ingestion_bucket(mock_S3_client):
    mock_S3_client.create_bucket(
        Bucket=bucket_name,
    CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
    mock_S3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)

    mock_data = {"name": "Mukund", "Team": 11, "project": "terraform"}
    assert write_to_ingestion_bucket(json.dump(mock_data), bucket_name, object_key) == {"name": "Mukund", "Team": 11, "project": "terraform"}



def test_that_function_modifies_the_data_read_from_s3_bucket(mock_S3_client):
    # timestamp = datetime.now()
    mock_S3_client.put_object(Bucket=bucket_name, Key=f"Test-key-{timestamp}", Body=mock_json)
    assert write_to_ingestion_bucket(
            json.loads(mock_data),
            bucket_name,
            object_key)["content1"] == expected_table

"""

def test_that_fucntion_writes_data_into_s3_bucket():
    pass
"""