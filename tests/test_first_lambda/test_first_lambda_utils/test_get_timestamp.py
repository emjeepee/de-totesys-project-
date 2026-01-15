import pytest
import logging
import os
import boto3

from unittest.mock import Mock, patch, ANY
from moto import mock_aws
from botocore.exceptions import ClientError


from src.first_lambda.first_lambda_utils.get_timestamp import get_timestamp
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
        # Create two buckets. The first will have nothing in it
        # (to allow the testing of function
        # get_timestamp() when its 'except:' code
        # executes).
        # The second will contain an object for the intital
        # timestamp for the year 1900 (to allow the testing of
        # function get_timestamp() when its 'try:'
        # code executes:
        test_bckt_1 = "ingestion-bucket-empty"
        test_bckt_2 = "ingestion-bucket-with-initial-1900-timestamp"

        # create the bucket that will contain no timestamp in the test:
        S3_client.create_bucket(
            Bucket=test_bckt_1,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # create the bucket that will contain a timestamp in the test:
        S3_client.create_bucket(
            Bucket=test_bckt_2,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )


        # get_timestamp() returns 
        # timestamp_1900 on the 
        # first ever run of the first 
        # lambda or when there has been 
        # a problem reading the ingestion 
        # bucket, ie the function 
        # returns timestamp_1900 when 
        # the except block of the 
        # function runs:
        timesstamp_1900 = "1900-01-01_00-00-00"  


        # get_timestamp() returns 
        # timestamp_2025 when the 
        # first lambda runs for the 
        # 2nd-plus time, ie this is 
        # returned by the try block 
        # of the function:
        timesstamp_2025 = "2025-06-04_00-00-00"  
        

        timestamp_key = "***current_timestamp***"

        # put 2025 timestamp into second bucket:
        S3_client.put_object(
            Bucket=test_bckt_2,
            Key=timestamp_key,
            Body=timesstamp_2025
                            )

        # yield stuff:
        yield_list = [
            S3_client,
            test_bckt_1,
            test_bckt_2,
            timesstamp_1900,
            timesstamp_2025,
            timestamp_key,
        ]
        yield yield_list



def test_get_timestamp_returns_a_string(S3_setup):
    # arrange
    yield_list = S3_setup
    expected_timestamp = yield_list[3]
    bucket = yield_list[1]  # the empty bucket
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    timestamp = get_timestamp(s3_client, 
                              bucket, 
                              ts_key, 
                              ts_1900)
    result = type(timestamp)

    # assert
    assert result == str





# @pytest.mark.skip
def test_get_timestamp_returns_default_timestamp_when_it_should(S3_setup):
    # arrange
    yield_list = S3_setup
    expected_timestamp = yield_list[3]
    bucket = yield_list[1]  # the empty bucket
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    result_timestamp = get_timestamp(s3_client, 
                                     bucket, 
                                     ts_key, 
                                     ts_1900)

    # assert
    assert result_timestamp == expected_timestamp




# @pytest.mark.skip
def test_get_timestamp_returns_correct_timestamp_when_lambda_runs_2ndPlus_time(
    S3_setup,
):
    # arrange
    yield_list = S3_setup
    expected_timestamp = yield_list[4]
    bucket = yield_list[2]  # bucket containing 2025 timestamp
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    result_timestamp = get_timestamp(s3_client, 
                                     bucket, 
                                     ts_key, 
                                     ts_1900)


    # assert
    assert result_timestamp == expected_timestamp



# @pytest.mark.skip
def test_returns_default_timestamp_on_raising_of_a_ClientError(S3_setup):
    # Arrange
    yield_list = S3_setup
    bucket = yield_list[2] # mock bucket, contains tmstmp "2025-06-04_00-00-00"
    s3_client = Mock()  # create a mock S3 client
    default_ts = yield_list[3]
    ts_key = yield_list[5]

    # Make get_object raise ClientError
    s3_client.get_object.side_effect = ClientError(
    {"Error": {"Code": "500", "Message": "Read of ingestion bucket failed"}},
    "get_object"
                                                  )

    # Act
    result = get_timestamp(s3_client, 
                           bucket, 
                           ts_key, 
                           default_ts)

    # Assert
    assert result == default_ts  # function should return default_ts on error
    s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=ts_key)


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
    expected_timestamp = yield_list[3]
    empty_bucket = yield_list[1]  # the empty bucket
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]


    mock_s3_client = Mock()
    mock_s3_client.get_object.side_effect = ClientError(
    {"Error": {"Code": "500", "Message": 
               "Failed to get object from ingestion bucket"}
        },
     "get_object"
                                                       ) 


    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="get_timestamp.py")

    get_timestamp(mock_s3_client,
                  empty_bucket, 
                  ts_key, 
                  'default_timestamp')

    assert any(errors_lookup['err_0'] in msg for msg in caplog.messages)
