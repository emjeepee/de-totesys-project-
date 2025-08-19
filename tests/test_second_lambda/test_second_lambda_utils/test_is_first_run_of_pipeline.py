import boto3
import pytest
import os
import json

from moto import mock_aws
from unittest.mock import Mock, patch, call
from botocore.exceptions import ClientError

from src.second_lambda.second_lambda_utils.is_first_run_of_pipeline import is_first_run_of_pipeline




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
        bucket_name_empty = "11-process-bucket_empty"
        bucket_name_not_empty = "11-process-bucket_not_empty"
        
        # Create two buckets, 
        # one that will be empty, the
        # other that will contain 
        # two objects:
        mock_S3_client.create_bucket( # empty bucket
            Bucket=bucket_name_empty,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )

        mock_S3_client.create_bucket( # bucket will contain two objects
            Bucket=bucket_name_not_empty,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )


        test_obj_1 = [{'test_1_key': 'test_1_val'}, {'test_2_key': 'test_2_val'}]
        test_obj_2 = [{'test_1_key': 'test_1_val'}, {'test_2_key': 'test_2_val'}]

        test_key_1 ='test_key_1.json'
        test_key_2 ='test_key_2.json'

        # put the two objects in the 
        # bucket
        mock_S3_client.put_object(Bucket = bucket_name_not_empty, Key = test_key_1, Body = json.dumps(test_obj_1))
        mock_S3_client.put_object(Bucket = bucket_name_not_empty, Key = test_key_2, Body = json.dumps(test_obj_2))


        yield mock_S3_client, bucket_name_empty, bucket_name_not_empty, test_obj_1, test_obj_2, test_key_1,  test_key_2     




# @pytest.mark.skip
def test_bucket_is_empty(general_setup):
    # Arrange
    (mock_S3_client, bucket_name_empty, bucket_name_not_empty, test_obj_1, test_obj_2, test_key_1,  test_key_2) = general_setup

    # Act
    # is_first_run_of_pipeline(proc_bucket: str, s3_client):
    result = False
    # result = is_first_run_of_pipeline(bucket_name_empty, mock_S3_client)
    
    

    # Assert
    assert result
