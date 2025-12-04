import boto3
import pytest
import os
import json

from moto import mock_aws
from unittest.mock import Mock, patch, call
from botocore.exceptions import ClientError

from src.second_lambda.second_lambda_utils.get_latest_table import get_latest_table




@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

# IMPORTANT!!!!! MAKE A NOTE: Moto persists 
# the bucket state across your tests 
# if fixture S3_setup has scope="module".
# Making the following fixture function-scoped 
# instead of module-scoped means each test gets
# a fresh Moto S3, which means every test runs 
# with a clean S3 environment:
@pytest.fixture(scope="function")
def general_setup():
    with mock_aws():
        mock_S3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "11-ingestion-bucket"
        
        mock_S3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )


        test_obj_1 = [{'design_1_key': 'design_1_val'}]
        test_obj_2 = [{'design_2_key': 'design_2_val'}]
        test_obj_3 = [{'design_3_key': 'design_3_val'}]


        test_key_1 = 'design/2025-06-02_22-17-19-2513.json' 
        test_key_2 = 'design/2025-05-29_22-17-19-2513.json' 
        test_key_3 = 'design/2025-04-02_22-17-19-2513.json'

        mock_S3_client.put_object(Bucket = bucket_name, Key = test_key_1, Body = json.dumps(test_obj_1))
        mock_S3_client.put_object(Bucket = bucket_name, Key = test_key_2, Body = json.dumps(test_obj_2))
        mock_S3_client.put_object(Bucket = bucket_name, Key = test_key_3, Body = json.dumps(test_obj_3))


        yield mock_S3_client, bucket_name, test_key_1, test_key_2, test_key_3, test_obj_1, test_obj_2, test_obj_3, bucket_name 





# @pytest.mark.skip
def test_returns_a_list(general_setup):
    # Arrange:
    (mock_S3_client, 
     bucket_name, 
     test_key_1, test_key_2, test_key_3, 
     test_obj_1, test_obj_2, test_obj_3, 
     bucket_name ) = general_setup

    expected = list

    # Act:
    # get_latest_table(s3_client, bucket: str, table_name: str)
    response = get_latest_table(mock_S3_client, bucket_name, 'design')
    result = type(response) 

    # Assert:
    assert result == expected







# @pytest.mark.skip
def test_returns_correct_list(general_setup):
    # Arrange:
    (mock_S3_client, 
     bucket_name, 
     test_key_1, test_key_2, test_key_3, 
     test_obj_1, test_obj_2, test_obj_3, 
     bucket_name ) = general_setup

    expected = test_obj_1

    # Act:
    result = get_latest_table(mock_S3_client, bucket_name, 'design')


    # Assert:
    assert result == expected






# @pytest.mark.skip
def test_raises_RuntimeError_when_list_objs_v2_fails(general_setup):
    # Arrange:
    (mock_S3_client, 
     bucket_name, 
     test_key_1, test_key_2, test_key_3, 
     test_obj_1, test_obj_2, test_obj_3, 
     bucket_name ) = general_setup

    mock_S3_client.list_objects_v2 = Mock(side_effect=ClientError(
    {"Error": {"Code": "500", "Message": "Failed to list objects in bucket"}},
    "ListObjectsV2"
                                        ))

    # Act and assert:
    with pytest.raises(RuntimeError):
        get_latest_table(mock_S3_client, bucket_name, 'design')



# @pytest.mark.skip
def test_raises_RuntimeError_when_get_object_fails(general_setup):
    # Arrange:
    (mock_S3_client, 
     bucket_name, 
     test_key_1, test_key_2, test_key_3, 
     test_obj_1, test_obj_2, test_obj_3, 
     bucket_name ) = general_setup

    mock_S3_client.get_object = Mock(side_effect=ClientError(
    {"Error": {"Code": "500", "Message": "Failed to get object in bucket"}},
    "GetObject"
                                        ))

    # Act and assert:
    with pytest.raises(RuntimeError):
        get_latest_table(mock_S3_client, bucket_name, 'design')
