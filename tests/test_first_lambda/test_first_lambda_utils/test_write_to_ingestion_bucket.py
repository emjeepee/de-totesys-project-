from moto import mock_aws
import boto3
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime
import os
from botocore.exceptions import ClientError
from src.first_lambda.first_lambda_utils.write_to_ingestion_bucket import write_to_ingestion_bucket







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
def test_function_write_to_ingestion_bucket_raises_correct_exception_if_called_function_get_most_recent_table_data_fails(
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
    file_location = "design/"

    error_response = {
        "Error": {
            "Code": "Error",
            "Message": "This is a mock ClientError exception raised by mocked version of get_most_recent_table_data().",
        }
    }

    client_error_from_mock_function = ClientError(error_response, "get_object")

    # act and assert:
    with patch(
        "src.utils_write_to_ingestion_bucket.get_most_recent_table_data",
        side_effect=client_error_from_mock_function,
    ):
        result = write_to_ingestion_bucket(ur_mdt2, bucket_name, file_location)

        assert result.response["Error"]["Message"] == error_response["Error"]["Message"]


def test_function_write_to_ingestion_bucket_raises_correct_exception_if_called_function_update_rows_in_table_fails(
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
    file_location = "design/"

    error_response = {
        "Error": {
            "Code": "Error",
            "Message": "This is a mock ClientError exception raised by mocked version of get_most_recent_table_data().",
        }
    }

    client_error_from_mock_function = ClientError(error_response, "get_object")

    # act and assert:
    with patch(
        "src.first_lambda.first_lambda_utils.get_most_recent_table_data",
        side_effect=client_error_from_mock_function,
    ):
        result = write_to_ingestion_bucket(ur_mdt2, bucket_name, file_location)

        assert result.response["Error"]["Message"] == error_response["Error"]["Message"]


def test_function_write_to_ingestion_bucket_raises_correct_exception_if_called_function_save_updated_table_to_S3_fails(
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
    file_location = "design"

    error_response = {
        "Error": {
            "Code": "Error",
            "Message": "This is a mock ClientError exception raised by the mocked version of save_updated_table_to_S3().",
        }
    }

    client_error_from_mock_function = ClientError(error_response, "put_object")

    # act and assert:
    with patch(
        "src.first_lambda.first_lambda_utils.save_updated_table_to_S3",
        side_effect=client_error_from_mock_function,
    ):
        result = write_to_ingestion_bucket(ur_mdt2, bucket_name, file_location)

        assert result.response["Error"]["Message"] == error_response["Error"]["Message"]


# integration testing:
# @pytest.mark.skip
def test_that_funcion_write_to_ingestion_bucket_correctly_integrates_utility_functions(
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

    # act:
    write_to_ingestion_bucket(ur_mdt2, bucket_name, "design")
    most_recent_table = get_most_recent_table_data("design", S3_client, bucket_name)

    # assert:
    assert most_recent_table[0]["team"] == 41
