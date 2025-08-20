import boto3
import pytest
import os
import json

from moto import mock_aws
from unittest.mock import Mock, patch, call
from botocore.exceptions import ClientError

from src.second_lambda.second_lambda_utils.upload_to_s3 import upload_to_s3



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
        bucket_name = "11-process-bucket_empty"
        
        
        # Create two buckets, 
        # one that will be empty, the
        # other that will contain 
        # two objects:
        mock_S3_client.create_bucket( # empty bucket
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )
        
        test_key = 'test-prefix/test.json'

        test_body = json.dumps([{'test_1': 1}, {'test_2': 2}, {'test_3': 3}])


        yield mock_S3_client, bucket_name, test_key, test_body






# @pytest.mark.skip
def test_uploads_with_correct_key(general_setup):
    # Arrange:
    (mock_S3_client, bucket_name, test_key, test_body) = general_setup
    expected = test_key

    # Act:
    upload_to_s3(mock_S3_client, bucket_name, test_key, test_body)
    response = mock_S3_client.list_objects_v2(Bucket = bucket_name, Prefix = 'test-prefix')
    result = response['Contents'][0]['Key']
    # result = None

    # Assert:
    assert result == expected





# @pytest.mark.skip
def test_uploads_correct_data(general_setup):
    # Arrange:
    (mock_S3_client, bucket_name, test_key, test_body) = general_setup
    expected_0 = json.loads(test_body)[0]
    expected_1 = json.loads(test_body)[1]
    expected_2 = json.loads(test_body)[2]


    # Act:
    upload_to_s3(mock_S3_client, bucket_name, test_key, test_body)
    response = mock_S3_client.get_object(Bucket = bucket_name, Key = test_key)
    json_py_list = response['Body'].read().decode("utf-8")
    py_list = json.loads(json_py_list)

    result_0 = py_list[0]
    result_1 = py_list[1]
    result_2 = py_list[2]
    # result_0 = None
    # result_1 = None
    # result_2 = None


    # Assert:
    assert result_0 == expected_0
    assert result_1 == expected_1
    assert result_2 == expected_2






# @pytest.mark.skip
def test_raises_RuntimeError(general_setup):
    # Arrange:
    (mock_S3_client, bucket_name, test_key, test_body) = general_setup

    mock_S3_client.put_object = Mock(side_effect=ClientError(
    {"Error": {"Code": "500", "Message": "Failed to upload object to bucket"}},
    "PutObject"
                                        ))

    with pytest.raises(RuntimeError):
        # return
        upload_to_s3(mock_S3_client, bucket_name, test_key, test_body)