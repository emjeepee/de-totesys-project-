import os
import boto3
import pytest
import json
import logging

from unittest.mock import Mock, patch
from moto import mock_aws

from botocore.exceptions import ClientError, BotoCoreError
from src.first_lambda.first_lambda_utils.save_updated_table_to_S3 import save_updated_table_to_S3
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup   


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="module")
def S3_setup():
    with mock_aws():
        S3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "11-ingestion-bucket"
        S3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_design_table = [
            {"design_id": 1, "name": "abdul", "team": 11, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 12, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 13, "project": "terraform"},
        ]

        # create mock jsonified table:
        mdt_json = json.dumps(mock_design_table)

        # create keys for mock jsonified table:
        key_1 = "design/2025-05-29_22-17-19-251352.json"
        key_2 = "design/2025-05-29_22-07-19-251352.json"
        key_3 = "design/2025-05-29_21-57-19-251352.json"


        yield S3_client, bucket_name, key_1, mock_design_table, mdt_json


@pytest.fixture(scope="module")
def S3_setup_put_obj_err(S3_setup):
    """
    Fixture that forces S3_client's 
    put_object() to raise an 
    exception.
    """
    S3_client, bucket_name, key_1, mock_design_table, mdt_json = S3_setup

    def mock_put_object(*args, **kwargs):
        raise ClientError(
            {"Error": 
             {"Code": "500", 
              "Message": "Forcing put_object() to raise exception"
              }},
            "put_object()",
        )
    S3_client.put_object = mock_put_object
    yield S3_client, bucket_name, key_1, mock_design_table, mdt_json






# @pytest.mark.skip
def test_function_save_updated_table_to_S3_saves_a_table_to_the_S3(S3_setup):
    (
        S3_client,
        bucket_name,
        key_1,
        mock_design_table,
        mdt_json,
    ) = S3_setup


    # arrange:

    
    # act:
    save_updated_table_to_S3(mdt_json, S3_client, key_1, bucket_name)
    response = S3_client.get_object(Bucket=bucket_name, Key=key_1)
    returned_table_json = response["Body"].read().decode("utf-8")
    returned_table = json.loads(returned_table_json)

    # assert:
    assert returned_table == mock_design_table


# @pytest.mark.skip
def test_function_save_updated_table_to_S3_raises_exception_if_attempt_to_put_object_in_S3_fails(
    S3_setup_put_obj_err,
                                                                                                ):
    (
        S3_client, 
        bucket_name, 
        key_1, 
        mock_design_table, 
        mdt_json
    ) = S3_setup_put_obj_err

    with pytest.raises(ClientError):
        # save_updated_table_to_S3( updated_table, S3_client: boto3.client, new_key: str, bucket: str ):
        save_updated_table_to_S3(mdt_json, S3_client, key_1, bucket_name)



# @pytest.mark.skip
def test_function_save_updated_table_to_S3_logs_correctly(
                        S3_setup_put_obj_err,
                        caplog
                                                         ):
    (
        S3_client, 
        bucket_name, 
        key_1, 
        mock_design_table, 
        mdt_json
    ) = S3_setup_put_obj_err

    caplog.set_level(logging.ERROR, logger="get_latest_table")

    with pytest.raises(ClientError):
        # save_updated_table_to_S3( updated_table, S3_client: boto3.client, new_key: str, bucket: str ):
        save_updated_table_to_S3(mdt_json, S3_client, key_1, bucket_name)        

    error_message = errors_lookup['err_5'] + 'design'
    assert any(error_message in msg for msg in caplog.messages)        


