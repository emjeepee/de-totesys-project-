from moto import mock_aws
import boto3
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime
import os
from src.utils_write_to_ingestion_bucket import write_to_ingestion_bucket, get_most_recent_table_data





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
        yield boto3.client("s3", region_name="eu-west-2")



def test_get_most_recent_table_data_returns_correct_list(mock_S3_client):
    # arrange:
    bucket_name = "11-ingestion-bucket"

    # Create mock bucket
    mock_S3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                                )

    # # Create object with mock data
    # body = '{"name": "abdul", "team": 11, "project": "terraform"}'

    # # mock data
    # body_1 = '{"name": "Neil", "team": 12, "project": "writing JSON"}'

    # create mock python tables
    mock_table_1 =  [
            {"name": "abdul", "team": 11, "project": "terraform"},
            {"name": "Mukund", "team": 12, "project": "terraform"},
            {"name": "Neil", "team": 13, "project": "terraform"},
                    ]

    mock_table_2 =  [
            {"name": "abdul", "team": 14, "project": "terraform"},
            {"name": "Mukund", "team": 15, "project": "terraform"},
            {"name": "Neil", "team": 16, "project": "terraform"},
                    ]

    mock_table_3 =  [
            {"name": "abdul", "team": 17, "project": "terraform"},
            {"name": "Mukund", "team": 18, "project": "terraform"},
            {"name": "Neil", "team": 19, "project": "terraform"},
                    ]

    # create mock jsonified tables:
    mt_1_json = json.dumps(mock_table_1)
    mt_2_json = json.dumps(mock_table_2)
    mt_3_json = json.dumps(mock_table_3)

    # create keys for mock jsonified tables:
    key_1 = 'design/2025-05-29_22-17-19-251352'
    key_2 = 'design/2025-05-29_22-07-19-251352'
    key_3 = 'design/2025-05-29_21-57-19-251352'

    # put the mock jsonified tables in the bucket:
    mock_S3_client.put_object(Bucket=bucket_name , Key= key_1, Body=mt_1_json)
    mock_S3_client.put_object(Bucket=bucket_name , Key= key_2, Body=mt_2_json)
    mock_S3_client.put_object(Bucket=bucket_name , Key= key_3, Body=mt_3_json)
    
    expected_table = mock_table_1

    # act:
    result = get_most_recent_table_data('design', mock_S3_client, bucket_name)

    # assert:
    assert expected_table == result 






@pytest.mark.skip
def test_it_can_read_object_from_ingestion_bucket(mock_S3_client):

    mock_S3_client.put_object(Bucket=bucket_name, Key=object_key, Body=body)

    mock_data = {"name": "Mukund", "Team": 11, "project": "terraform"}
    assert write_to_ingestion_bucket(json.dump(mock_data), bucket_name, object_key) == {
        "name": "Mukund",
        "Team": 11,
        "project": "terraform",
    }


@pytest.mark.skip
def test_that_function_modifies_the_data_read_from_s3_bucket(mock_S3_client):
    # timestamp = datetime.now()
    mock_S3_client.put_object(
        Bucket=bucket_name, Key=f"Test-key-{timestamp}", Body=mock_json
    )
    assert write_to_ingestion_bucket(json.loads(body_1), bucket_name, object_key)["content1"] == expected_table
    


"""

def test_that_fucntion_writes_data_into_s3_bucket():
    pass
"""
