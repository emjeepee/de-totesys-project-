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
        # first ever run of the first lambda, 
        # ie this is returned by the 
        # 'except:' code of the function 
        # under test.
        timesstamp_1900 = "1900-01-01_00-00-00"  


        # change_after_time_timestamp()
        # returns timesstamp_2025 when the 
        # first lambda runs for the 
        # 2nd-plus time, ie this is 
        # returned by the 'try:' code of
        # the function being tested.
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



# @pytest.mark.skip
def test_that_function_change_after_time_timestamp_returns_default_timestamp_on_appropriate_occasion(
    S3_setup,
                                                                                    ):
    # arrange
    yield_list = S3_setup
    expected_timestamp = yield_list[3]
    bucket = yield_list[1]  # the empty bucket
    s3_client = yield_list[0]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    result_timestamp = change_after_time_timestamp(bucket, s3_client, ts_key, ts_1900)

    # assert
    assert result_timestamp == expected_timestamp


# @pytest.mark.skip
def test_that_function_change_after_time_timestamp_returns_correct_timestamp_when_lambda_runs_2ndPlus_time(
    S3_setup,
):
    # arrange

    yield_list = S3_setup
    expected_timestamp = yield_list[4]
    bucket = yield_list[2]  # the bucket containing the 2025 timestamp
    s3_client = yield_list[0]
    ts_2025 = yield_list[4]
    ts_1900 = yield_list[3]
    ts_key = yield_list[5]

    # act
    result_timestamp = change_after_time_timestamp(bucket, s3_client, ts_key, ts_1900)

    # assert
    assert result_timestamp == expected_timestamp


# @pytest.mark.skip
def test_that_function_change_after_time_timestamp_saves_new_timestamp_to_bucket_real(
    S3_setup,
):
    # arrange
    yield_list = S3_setup
    # expected_timestamp = yield_list[4]
    bucket = yield_list[2]  # the bucket containing 2025 timestamp "2025-06-04_00-00-00"
    s3_client = yield_list[0]
    default_ts = yield_list[3]
    ts_key = yield_list[5]

    # act
    with patch("src.first_lambda.first_lambda_utils.change_after_time_timestamp.datetime") as mock_datetime:
        mock_now = Mock()
        mock_now.isoformat.return_value = "2025-06-04T08:28:12.301474+00:00"
        mock_datetime.now.return_value = mock_now

        expected_time = "2025-06-04T08:28:12"
        # Act:
        # get the previous stored timestamp:
        change_after_time_timestamp(bucket, s3_client, ts_key, default_ts)
        response = s3_client.get_object(Bucket=bucket, Key=ts_key)
        result_time = response["Body"].read().decode("utf-8")

        # Assert
        assert result_time == expected_time







# @pytest.mark.skip
def test_change_after_time_timestamp_raises_ClientError_error_for_get_object(S3_setup):
    # Arrange
    yield_list = S3_setup
    bucket = yield_list[2]
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
    assert result == default_ts  # the function should return default_ts on error
    s3_client.get_object.assert_called_once_with(Bucket=bucket, Key=ts_key)



# @pytest.mark.skip
def test_change_after_time_timestamp_raises_ClientError_error_for_put_object(S3_setup):
    # Arrange
    yield_list = S3_setup
    bucket = yield_list[2]
    s3_client = Mock()  # create a mock S3 client
    default_ts = yield_list[3]
    ts_key = yield_list[5]

    # Make get_object raise ClientError
    s3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "Write to ingestion bucket failed"}},
        "get_object"
    )

    # Act
    result = change_after_time_timestamp(bucket, s3_client, ts_key, default_ts)

    # Assert
    assert result == default_ts  # the function should return default_ts on error
    s3_client.put_object.assert_called_once_with(Bucket=bucket, Key=ts_key, Body=ANY)