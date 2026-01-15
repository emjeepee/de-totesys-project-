import pytest
import logging
import os
import boto3

from unittest.mock import Mock, patch, ANY
from moto import mock_aws
from botocore.exceptions import ClientError


from src.first_lambda.first_lambda_utils.replace_timestamp import replace_timestamp
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
        # Create a bucket that contains
        # a timestamp string:
        test_bckt_1 = "ingestion-bucket-with-timestamp"

        # create the bucket that will contain no timestamp in the test:
        S3_client.create_bucket(
            Bucket=test_bckt_1,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # the timestamp the mock 
        # bucket contains initially 
        # and that get_timestamp 
        # must replace:
        timesstamp_june_2025 = "2025-06-04_00-00-00"  

        # the timestamp with which 
        # replace_timestamp will 
        # replace the original 
        # timestamp:
        timesstamp_july_2025 = "2025-07-04_00-00-00"  



        timestamp_key = "***current_timestamp***"

        # put june 2025 timestamp 
        # into bucket:
        S3_client.put_object(
            Bucket=test_bckt_1,
            Key=timestamp_key,
            Body=timesstamp_june_2025
                            )

        # yield stuff:
        yield_list = [
            S3_client,
            test_bckt_1,
            timesstamp_june_2025,
            timesstamp_july_2025,
            timestamp_key,
        ]
        yield yield_list



# @pytest.mark.skip
def test_replace_timestamp_replaces_original_timestamp( S3_setup):
    # arrange
    yield_list = S3_setup
    s3_client = yield_list[0]
    bucket = yield_list[1]  
    expected_timestamp = yield_list[3] # july timestamp
    ts_key = yield_list[4]

    # act
    replace_timestamp(s3_client, 
                      bucket, 
                      ts_key, 
                      yield_list[3])

    response = s3_client.get_object(Bucket=bucket, 
                                    Key=ts_key)
    result_timestamp = response["Body"].read().decode("utf-8")


    # assert
    assert result_timestamp == expected_timestamp





# pass in pytest fixtures passed as 
# args to the test function. the 
# order is not important:
def test_logs_exception(caplog, S3_setup):
    """
    caplog is a built-in pytest 
    fixture that captures 
    anything written to the 
    Python logging system 
    during the test.
    """


    # arrange
    yield_list = S3_setup
    s3_client = yield_list[0]
    bucket = yield_list[1]  
    expected_timestamp = yield_list[3] # july timestamp
    ts_key = yield_list[4]


    mock_s3_client = Mock()
    mock_s3_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "500", "Message": 
               "Failed to put object into ingestion bucket"}
            },
     "put_object"
                                                       ) 


    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="replace_timestamp.py")

    with pytest.raises(ClientError):
        replace_timestamp(mock_s3_client,
                          bucket, 
                          ts_key, 
                          yield_list[3])

    assert any(errors_lookup['err_1'] in msg for msg in caplog.messages)
