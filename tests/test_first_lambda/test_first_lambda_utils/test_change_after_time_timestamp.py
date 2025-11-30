import pytest
from unittest.mock import Mock, patch, ANY
from moto import mock_aws
from botocore.exceptions import ClientError

import os
import boto3

from src.first_lambda.first_lambda_utils.change_after_time_timestamp import change_after_time_timestamp


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
        # change_after_time_timestamp() when its 'except:' code
        # executes).
        # The second will contain an object for the intital
        # timestamp for the year 1900 (to allow the testing of
        # function change_after_time_timestamp() when its 'try:'
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


        # change_after_time_timestamp()
        # returns timesstamp_1900 on the 
        # first ever run of the first 
        # lambda or when there has been 
        # a problem reading the ingestion 
        # bucket, ie the function 
        # returns timesstamp_1900 when 
        # the first except block of the 
        # function runs:
        timesstamp_1900 = "1900-01-01_00-00-00"  


        # change_after_time_timestamp()
        # returns timesstamp_2025 when the 
        # first lambda runs for the 
        # 2nd-plus time, ie this is 
        # returned by the first try block 
        # of the function.
        timesstamp_2025 = "2025-06-04_00-00-00"  
        

        timestamp_key = "***current_timestamp***"

        # put 2025 timestamp into second bucket:
        S3_client.put_object(
            Bucket=test_bckt_2, Key=timestamp_key, Body=timesstamp_2025
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



def test_change_after_time_timestamp_returns_a_string(S3_setup):
    # arrange
    expected_fail = list
    yield_list = S3_setup
    expected_timestamp = yield_list[3]
    bucket = yield_list[1]  # the empty bucket
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    timestamp = change_after_time_timestamp(bucket, s3_client, ts_key, ts_1900)
    result = type(timestamp)

    # assert
    # ensure the test can fail:
    # assert result == expected_fail
    assert result == str





# @pytest.mark.skip
def test_change_after_time_timestamp_returns_default_timestamp_when_it_should(S3_setup):
    # arrange
    expected_fail = "fail_test"
    yield_list = S3_setup
    expected_timestamp = yield_list[3]
    bucket = yield_list[1]  # the empty bucket
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    result_timestamp = change_after_time_timestamp(bucket, s3_client, ts_key, ts_1900)

    # assert
    # ensure the test can fail:
    # assert result_timestamp == expected_fail
    assert result_timestamp == expected_timestamp




# @pytest.mark.skip
def test_change_after_time_timestamp_returns_correct_timestamp_when_lambda_runs_2ndPlus_time(
    S3_setup,
):
    # arrange
    expected_fail = "fail_test"
    yield_list = S3_setup
    expected_timestamp = yield_list[4]
    bucket = yield_list[2]  # bucket containing 2025 timestamp
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    result_timestamp = change_after_time_timestamp(bucket, s3_client, ts_key, ts_1900)

    # assert
    # ensure test can fail:
    # assert result_timestamp == expected_fail
    assert result_timestamp == expected_timestamp


# @pytest.mark.skip
def test_change_after_time_timestamp_saves_new_timestamp_to_bucket_real(
    S3_setup,
                        ):
    """
    This test function: 
        Uses the mock bucket that already 
        contains a ('old') timestamp.

        Mocks a current ('new') timestamp,
        which change_after_time_timestamp()
        should put into the S3 bucket as a 
        replacement for the old timestamp.

        Tests that the new timestamp is  
        actually in the mock ingestion 
        bucket.
    """

    # arrange
    expected_fail = "fail_test"
    yield_list = S3_setup
    # expected_timestamp = yield_list[4]
    bucket = yield_list[2]  # bucket containing timestamp "2025-06-04_00-00-00"
    s3_client = yield_list[0]
    default_ts = yield_list[3]
    ts_key = yield_list[5]

    # act
    with patch("src.first_lambda.first_lambda_utils.change_after_time_timestamp.datetime") as mock_datetime:
        mock_now_datetime = Mock()
        mock_datetime.now.return_value = mock_now_datetime
        mock_now_datetime.isoformat.return_value = "2025-06-04T01:23:45.678910+00:00" # mocks now_ts but with seconds
        

        expected_time = "2025-06-04T01:23:45"
        # Act:
        # put the new timestamp
        # in the mock ingestion bucket,
        # replacing the old one:
        change_after_time_timestamp(bucket, s3_client, ts_key, default_ts)
        # Get the timestamp in the mock 
        # ingestion bucket and test that 
        # it is correct:
        response = s3_client.get_object(Bucket=bucket, Key=ts_key)
        result_time = response["Body"].read().decode("utf-8")

        # Assert
        # ensure the test can fail:
        # assert result_time == expected_fail
        assert result_time == expected_time




# @pytest.mark.skip
def test_change_after_time_timestamp_raises_ClientError_error_for_get_object(S3_setup):
    # Arrange
    expected_fail = 'fail_test'
    yield_list = S3_setup
    bucket = yield_list[2] # mock bucket containing timestamp "2025-06-04_00-00-00"
    s3_client = Mock()  # create a mock S3 client
    default_ts = yield_list[3]
    ts_key = yield_list[5]

    # Make get_object raise ClientError
    s3_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "Read of ingestion bucket failed"}},
        "get_object"
    )

    # Act
    result = change_after_time_timestamp(bucket, s3_client, ts_key, default_ts)

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == default_ts  # the function should return default_ts on error
    s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=ts_key)



# @pytest.mark.skip
def test_change_after_time_timestamp_raises_ClientError_error_for_put_object(S3_setup):
    # Arrange
    expected_fail = 'fail_test'
    yield_list = S3_setup
    bucket = yield_list[2] # mock bucket containing timestamp "2025-06-04_00-00-00"
    s3_client = Mock()  # create a mock S3 client
    default_ts = yield_list[3]
    ts_key = yield_list[5]


    # This test must mock s3_client.get_object too
    # because the function returns what get_object()
    # returns:
    # Need to mock this:
    # response["Body"].read().decode("utf-8"),
    # which is what get_object() returns

    mock_body = Mock()
    mock_body_read_return = Mock()
    mock_body.read.return_value = mock_body_read_return
    mock_body_read_return.decode.return_value = '1900-01-01_00-00-00'
    # mock_body.read.decode = 'mock_timestamp'
    mock_dict = {"Body": mock_body}
    s3_client.get_object.return_value = mock_dict

    # Make get_object raise ClientError
    s3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "Write to ingestion bucket failed"}},
        "get_object"
    )

    # Act
    result = change_after_time_timestamp(bucket, s3_client, ts_key, default_ts)

    # Assert
    # ensure test can fail:
    # assert result == expected_fail    
    assert result == default_ts  # the function should return default_ts 
    s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=ts_key, Body=ANY)