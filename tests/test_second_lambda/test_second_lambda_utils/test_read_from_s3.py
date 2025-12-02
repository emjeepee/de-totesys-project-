import boto3
import pytest
import os
import json
import logging

from moto import mock_aws
from unittest.mock import Mock, patch, call
from botocore.exceptions import ClientError

from src.second_lambda.second_lambda_utils.read_from_s3 import read_from_s3
from src.second_lambda.second_lambda_utils.errors_lookup import errors_lookup



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
        bucket_name = "11-process-bucket"
        
        mock_S3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )

        test_obj = [{'test_1_key': 'test_1_val'}, {'test_2_key': 'test_2_val'}]

        test_key ='test_key.json'

        mock_S3_client.put_object(Bucket = bucket_name, Key = test_key, Body = json.dumps(test_obj))


        yield mock_S3_client, bucket_name, test_obj, test_key  




# @pytest.mark.skip
def test_returns_correct_object(general_setup):
    # Arrange
    (mock_S3_client, bucket_name, test_obj, test_key) = general_setup

    expected = test_obj

    # Act
    response = read_from_s3(mock_S3_client, bucket_name, test_key)             
    # ensure test can fail:
    # result = None
    result = json.loads(response)

    # Assert
    assert result == expected



# @pytest.mark.skip
def test_ClientError_causes_error_logging(general_setup, caplog):
    # arrange:
    (mock_S3_client, bucket_name, test_obj, test_key) = general_setup
    
    mock_S3_client.get_object = Mock(side_effect=ClientError(
    {"Error": {"Code": "500", "Message": "Failed to get object in bucket"}},
    "GetObject"
                                        ))
    
    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="read_from_s3")


    with pytest.raises(ClientError):
        read_from_s3(mock_S3_client, bucket_name, test_key)

    # ensure test can fail:
    # assert any('fail_message' in msg for msg in caplog.messages)        
    assert any(errors_lookup['err_0'] in msg for msg in caplog.messages)        