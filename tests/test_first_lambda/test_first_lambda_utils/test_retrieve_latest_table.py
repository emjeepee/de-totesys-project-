import pytest

import logging
import os
import boto3
import json

from unittest.mock import Mock, patch, ANY
from moto import mock_aws
from botocore.exceptions import ClientError


from src.first_lambda.first_lambda_utils.retrieve_latest_table import retrieve_latest_table
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
        # Create a bucket that will
        # contain an object under the 
        # latest-table key:
        mock_bckt_name = "ingestion-bucket-with-object"
        S3_client.create_bucket(
            Bucket=mock_bckt_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                               )

        timestamp = "2026-01-01_00-00-00"  
        table_name = 'mock_table'
        latest_table_key = f'{timestamp}/{table_name}'

        mock_table_python = {'mock_table': [{'row_1': 'x'}, 
                                            {'row_2': 'y'}, 
                                            {'row_3': 'z'}]}
        mock_table_json = json.dumps(mock_table_python)

        # put 2026 timestamp 
        # into bucket:
        S3_client.put_object(
            Bucket=mock_bckt_name,
            Key=latest_table_key,
            Body=mock_table_json
                            )

        # yield stuff:
        yield_list = [
            S3_client,
            mock_bckt_name,
            latest_table_key,
            mock_table_python,
            mock_table_json,
            table_name
                      ]
        
        yield yield_list



def test_retrieve_latest_table_returns_correct_table(S3_setup):
    # arrange
    yield_list        = S3_setup
    s3_client         = yield_list[0]
    bucket            = yield_list[1]  
    key               = yield_list[2]
    mock_table_python = yield_list[3]
    mock_table_json   = yield_list[4]
    table_name        = yield_list[5]

    # act
    result = retrieve_latest_table(s3_client, 
                                     bucket, 
                                     key, 
                                     table_name)
    
    # assert
    assert result == mock_table_python





# @pytest.mark.skip
def test_retrieve_latest_table_raises_exception_and_logs_correctly(caplog, 
                                                                   S3_setup):
    # arrange
    yield_list        = S3_setup
    s3_client         = yield_list[0]
    bucket            = yield_list[1]  
    key               = yield_list[2]
    mock_table_python = yield_list[3]
    mock_table_json   = yield_list[4]
    table_name        = yield_list[5]

    mock_s3_client = Mock()
    mock_s3_client.get_object.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": 
               "Failed to get object from ingestion bucket"}
        },
        "get_object"
                                                       ) 

    # assert
    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="retrieve_latest_table.py")

    # act
    # because the function 
    # re-raises the error:
    with pytest.raises(ClientError): 
        response = retrieve_latest_table(mock_s3_client, 
                                     bucket, 
                                     key, 
                                     table_name)

    err_message = errors_lookup["err_5"] + f"{table_name}"
    assert any(err_message in msg for msg in caplog.messages)





