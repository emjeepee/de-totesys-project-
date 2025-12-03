
import boto3
import pytest
import os


from moto import mock_aws
from unittest.mock import Mock, patch, call
from datetime import datetime, timedelta

from src.third_lambda.third_lambda_utils.third_lambda_init import third_lambda_init





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
        bucket_name = "11-processed-bucket"
        
        # Mock two buckets, one that will be empty
        # one that will contain three tables:
        mock_S3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )

        # object_key = event["Records"][0]["s3"]["object"]["key"]
        # make mock event:
        mock_event = {
                    'Records': [
                    {"s3": 
                     { "object": {
                         'key': "design/2025-06-13_13:23.parquet" # or fact_sales_order/2025-06-13_13-23.parquet
                                 },
                        "bucket": {
                                 "name": "11-processed-bucket",
                                 "ownerIdentity": {
                                 "principalId": "EXAMPLE"
                                                  },
                                  },                                 
                     },
                    }
                              ]
                }
        
        # mock conn_to_db()
        # and its return value: 
        mock_ctdb = Mock()
        mock_ctdb.return_value = 'conn'

        # mock close_db():
        mock_cldb = Mock()
        

        yield mock_event, mock_S3_client, mock_ctdb, mock_cldb




# @pytest.mark.skip
def test_returns_dict(general_setup):
    # Arrange:
    (mock_event, mock_S3_client, mock_ctdb, mock_cldb) = general_setup
    expected = dict
    expected_fail = list

    # Act:
    # third_lambda_init(event, conn_to_db, close_db, s3_client)
    response = third_lambda_init(mock_event, mock_ctdb, mock_cldb, mock_S3_client)
    result = type(response)
    # result = None

    #Assert:
    # enusre test can fail:
    # assert result == expected_fail
    assert result == expected




# @pytest.mark.skip
def test_returns_dict_with_correct_keys_and_values(general_setup):
    # Arrange:
    (mock_event, 
     mock_S3_client, 
     mock_ctdb, 
     mock_cldb) = general_setup
    
    expected_1 = mock_S3_client
    expected_2 = "design/2025-06-13_13:23.parquet"
    expected_3 = '11-processed-bucket'
    expected_4 = 'design'
    expected_5 = mock_ctdb()
    expected_6 = mock_cldb
    expected_fail = 'fail'


        # 's3_client': boto3.client("s3"),                        # boto3 S3 client
        # 'object_key': object_key,                               # key for Parquet file in processed bucket 
        # 'proc_bucket': ["Records"][0]["s3"]["bucket"]["name"],  # name of processed bucket
        # 'table_name': object_key.split("/")[0],                 # name of Parquet file in processed bucket
        # 'conn': conn_to_db('WAREHOUSE'),                        # pg8000.native Connection object
        # 'close_db': close_db 
    # Act:
    response = third_lambda_init(mock_event, 
                                 mock_ctdb, 
                                 mock_cldb, 
                                 mock_S3_client)
    
    result_1 = response['s3_client']
    result_2 = response['object_key']
    result_3 = response['proc_bucket']
    result_4 = response['table_name']
    result_5 = response['conn']
    result_6 = response['close_db']
    # result_fail = None

    #Assert:
    # ensure test can fail:
    # assert result_1 == expected_fail
    assert result_1 == expected_1
    # assert result_2 == expected_fail
    assert result_2 == expected_2
    # assert result_3 == expected_fail
    assert result_3 == expected_3
    # assert result_4 == expected_fail
    assert result_4 == expected_4
    # assert result_5 == expected_fail
    assert result_5 == expected_5
    # assert result_6 == expected_fail
    assert result_6 == expected_6

