import pytest 
import os
import boto3
import json
import logging

from unittest.mock import Mock, patch, ANY
from moto import mock_aws
from botocore.exceptions import ClientError


from src.first_lambda.first_lambda_utils.is_first_run_of_pipeline import is_first_run_of_pipeline
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup



# scope="function" ensures this setup runs 
# before every test function that uses it.
# 'aws_credentials' is the name of the 
# fixture:
@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def S3_setup(aws_credentials):
    with mock_aws():
        S3_client = boto3.client("s3", region_name="eu-west-2")
        # Create two buckets. The 
        # first will have nothing 
        # in it. The second will 
        # contain an object:
        mock_empty_bckt     = "processed-bucket-empty"
        mock_not_empty_bckt = "processed-bucket-with-object"

        # create the empty bucket:
        S3_client.create_bucket(
            Bucket=mock_empty_bckt,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # create the bucket that 
        # will contain an object:
        S3_client.create_bucket(
            Bucket=mock_not_empty_bckt,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                                )

        # make object to put 
        # into bucket:
        obj = json.dumps([{}, {}, {}])

        object_key = "some_key"

        # put 2025 timestamp into second bucket:
        S3_client.put_object(
            Bucket=mock_not_empty_bckt, Key=object_key, Body=obj
                            )

        # yield stuff:
        yield_list = [
            S3_client,
            mock_empty_bckt,
            mock_not_empty_bckt,
            obj,
            object_key,
        ]
        yield yield_list




def test_returns_a_boolean(S3_setup):
    # arrange:
    S3_client, mock_empty_bckt, mock_not_empty_bckt, obj, object_key = S3_setup
    expected = bool

    # act:
    response = is_first_run_of_pipeline(mock_empty_bckt, S3_client)
    result = type(response)

    # assert:
    assert result == expected




def test_returns_True_if_bucket_empty(S3_setup):
    # arrange:
    S3_client, mock_empty_bckt, mock_not_empty_bckt, obj, object_key = S3_setup
    expected = True

    # act:
    result = is_first_run_of_pipeline(mock_empty_bckt, S3_client)

    # assert:
    assert result == expected



    
def test_returns_False_if_bucket_not_empty(S3_setup):
    # arrange:
    S3_client, mock_empty_bckt, mock_not_empty_bckt, obj, object_key = S3_setup
    expected = False

    # act:
    result = is_first_run_of_pipeline(mock_not_empty_bckt, S3_client)

    # assert:
    assert result == expected

    

def test_raises_exception_and_logs_correctly(S3_setup, caplog):
    # arrange:
    S3_client, mock_empty_bckt, mock_not_empty_bckt, obj, object_key = S3_setup
    
    # Force get_object to fail
    S3_client.list_objects_v2 = Mock(
        side_effect=ClientError(
            error_response={
                "Error": {
                    "Code": "NoSuchKey",
                    "Message": "The specified key does not exist"
                         }
            },
            operation_name="GetObject",
        )
    )

    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="is_first_run_of_pipeline")


    with pytest.raises(ClientError):
        # act:
        result = is_first_run_of_pipeline(mock_not_empty_bckt, S3_client)

    # assert:
    assert any(errors_lookup['err_2'] in msg for msg in caplog.messages)           
    
