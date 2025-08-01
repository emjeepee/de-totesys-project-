from moto import mock_aws
import boto3
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime
import os
import re
from botocore.exceptions import ClientError
from src.first_lambda.first_lambda_utils.get_most_recent_table_data import get_most_recent_table_data
   


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

        mock_design_table_1 = [
            {"design_id": 1, "name": "abdul", "team": 11, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 12, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 13, "project": "terraform"},
        ]

        mock_design_table_2 = [
            {"design_id": 1, "name": "abdul", "team": 31, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 32, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 33, "project": "terraform"},
        ]

        updated_rows_of_mdt2 = [
            {"design_id": 1, "name": "abdul", "team": 41, "project": "terraform"}
        ]

        mock_table_1 = [
            {"name": "abdul", "team": 11, "project": "terraform"},
            {"name": "Mukund", "team": 12, "project": "terraform"},
            {"name": "Neil", "team": 13, "project": "terraform"},
        ]

        mock_table_2 = [
            {"name": "abdul", "team": 14, "project": "terraform"},
            {"name": "Mukund", "team": 15, "project": "terraform"},
            {"name": "Neil", "team": 16, "project": "terraform"},
        ]

        mock_table_3 = [
            {"name": "abdul", "team": 17, "project": "terraform"},
            {"name": "Mukund", "team": 18, "project": "terraform"},
            {"name": "Neil", "team": 19, "project": "terraform"},
        ]

        # create mock jsonified tables:
        mt_1_json = json.dumps(mock_table_1)
        mt_2_json = json.dumps(mock_table_2)
        mt_3_json = json.dumps(mock_table_3)
        mdt_1_json = json.dumps(mock_design_table_1)
        mdt_2_json = json.dumps(mock_design_table_2)
        ur_mdt2 = json.dumps(updated_rows_of_mdt2)

        # create keys for mock jsonified tables:
        key_1 = "design/2025-05-29_22-17-19-251352"
        key_2 = "design/2025-05-29_22-07-19-251352"
        key_3 = "design/2025-05-29_21-57-19-251352"
        key_mdt = "design/2025-05-29_23-57-19-251352"
        key_mdt2 = "design/2025-06-29_03-57-19-251352"

        S3_client.put_object(Bucket=bucket_name, Key=key_1, Body=mt_1_json)
        S3_client.put_object(Bucket=bucket_name, Key=key_2, Body=mt_2_json)
        S3_client.put_object(Bucket=bucket_name, Key=key_3, Body=mt_3_json)
        S3_client.put_object(Bucket=bucket_name, Key=key_mdt, Body=mdt_1_json)

        yield S3_client, bucket_name, mock_table_1, mock_table_2, mock_table_3, key_1, key_2, key_3, key_mdt, mock_design_table_1, mdt_2_json, key_mdt2, updated_rows_of_mdt2, ur_mdt2

# @pytest.mark.skip
def test_function_save_updated_table_to_S3_saves_a_table_to_the_S3(S3_setup):
    (
        S3_client,
        bucket_name,
        mock_table_1,
        mock_table_2,
        mock_table_3,
        key_1,
        key_2,
        key_3,
        key_mdt,
        mock_design_table_1,
        mdt_2_json,
        key_mdt2,
        updated_rows_of_mdt2,
        ur_mdt2,
    ) = S3_setup
    # arrange:

    new_key = "design/12345.json"

    # act:
    save_updated_table_to_S3(mdt_2_json, S3_client, new_key, bucket_name)
    response = S3_client.get_object(Bucket=bucket_name, Key=new_key)
    returned_table_string = response["Body"].read().decode("utf-8")
    returned_table = json.loads(returned_table_string)

    # assert:
    assert returned_table[0]["team"] == 31


# @pytest.mark.skip
def test_function_save_updated_table_to_S3_raises_exception_if_attempt_to_put_object_in_S3_fails(
    S3_setup,
):
    (
        S3_client,
        bucket_name,
        mock_table_1,
        mock_table_2,
        mock_table_3,
        key_1,
        key_2,
        key_3,
        key_mdt,
        mock_design_table_1,
        mdt_2_json,
        key_mdt2,
        updated_rows_of_mdt2,
        ur_mdt2,
    ) = S3_setup
    # arrange:
    error_response = {
        "Error": {
            "Code": "Error",
            "Message": "Error when function save_updated_table_to_S3() tried to save updated table to S3.",
        }
    }
    # make a mock object:
    mock_s3 = Mock()
    # make mock object have method put_object
    # that has side effect that is
    # botocore.exceptions ClientError object:
    mock_s3.put_object.side_effect = ClientError(error_response, "put_object")

    # act:
    # Patch boto3.client to return the mock
    with patch("boto3.client", return_value=mock_s3):
        # the args for save_updated_table_to_S3 are:
        # a jsonified updated table, the S3_client, a key for the object, the bucket name:
        result = save_updated_table_to_S3(mdt_2_json, mock_s3, key_2, bucket_name)
        # assert:
        assert (
            result.response["Error"]["Message"]
            == "Error when function save_updated_table_to_S3() tried to save updated table to S3."
        )
