from moto import mock_aws
import boto3
import pytest
import json
from unittest.mock import Mock, patch
from datetime import datetime
import os
import re
from botocore.exceptions import ClientError
from src.utils_write_to_ingestion_bucket import (
    write_to_ingestion_bucket,
    get_most_recent_table_data,
    create_formatted_timestamp,
    update_rows_in_table,
    save_updated_table_to_S3,
)


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
def test_get_most_recent_table_data_returns_correct_list(S3_setup):
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
    expected_table = mock_design_table_1

    # act:
    result = get_most_recent_table_data("design", S3_client, bucket_name)

    # assert:
    assert expected_table == result




# @pytest.mark.skip
def test_function_create_formatted_timestamp_creates_correct_timestamp(S3_setup):

    # arrange:
    formatted_now_string = create_formatted_timestamp()

    pattern = r"\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}"

    # act:

    # assert:
    assert re.match(pattern, formatted_now_string)




# @pytest.mark.skip
def test_function_update_rows_in_table_correctly_updates_a_table(S3_setup):
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
    update_rows = [
        {"design_id": 1, "name": "abdul", "team": 24, "project": "terraform"}
    ]
    # arrange:
    updated_table = update_rows_in_table(update_rows, mock_design_table_1, "design")

    result_row = [item for item in updated_table if item["name"] == "abdul"][0]
    # result_row will be this dictionary: {"name": "abdul", "team": 24, "project": "terraform"}

    # assert
    assert result_row["team"] == 24




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
def test_function_save_updated_table_to_S3_raises_exception_if_attempt_to_put_object_in_S3_fails(S3_setup):
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
        'Error': {
            'Code': 'Error',
            'Message': 'Error when function save_updated_table_to_S3() tried to save updated table to S3.'
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
        assert result.response['Error']['Message'] == 'Error when function save_updated_table_to_S3() tried to save updated table to S3.' 





# @pytest.mark.skip
def test_function_get_most_recent_table_data_raises_exception_if_attempt_to_list_objects_in_S3_fails(S3_setup):
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
        'Error': {
            'Code': 'Error',
            'Message': 'Error when function get_most_recent_table_data() tried to list objects in the S3 bucket.'
                }
                     }
    mock_s3 = Mock()
    mock_s3.list_objects_v2.side_effect = ClientError(error_response, "list_objects_v2")

    # act: 
    with patch("boto3.client", return_value=mock_s3):
        # the args for save_updated_table_to_S3 are: 
        # file_location: str, S3_client, bucket_name: str:
        result = get_most_recent_table_data('design', mock_s3, bucket_name)
    # assert:
        assert result.response['Error']['Message'] == error_response['Error']['Message']
    



# @pytest.mark.skip
def test_function_get_most_recent_table_data_raises_exception_if_attempt_get_an_object_in_S3_by_key_fails(S3_setup):
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
    bucket_name = "test-bucket"

    # Make fake S3 object keys of the kind that 
    # S3_client.list_objects_v2() would return
    # (only one is needed really):
    fake_keys = {
        "Contents": [
            {"Key": key_2}
                    ]
                }

    error_response = {
        'Error': {
            'Code': 'Error',
            'Message': 'Error when function get_most_recent_table_data() tried to get an object in the S3 bucket by key.'
                }
                     }
    
    mock_s3 = Mock()
    mock_s3.list_objects_v2.return_value = fake_keys

    mock_s3.get_object.side_effect = ClientError(error_response, "get_object")

    # act: 
    # the args for save_updated_table_to_S3 are: 
    # file_location: str, S3_client, bucket_name: str:
    result = get_most_recent_table_data(file_location, mock_s3, bucket_name)
    # assert:
    assert result.response['Error']['Message'] == error_response['Error']['Message']





# @pytest.mark.skip
def test_function_write_to_ingestion_bucket_raises_correct_exception_if_called_function_get_most_recent_table_data_fails(S3_setup):
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
        'Error': {
            'Code': 'Error',
            'Message': 'This is a mock ClientError exception raised by mocked version of get_most_recent_table_data().'
                }
                     }
    
    client_error_from_mock_function = ClientError(error_response, "get_object")

    # act and assert: 
    with patch('src.utils_write_to_ingestion_bucket.get_most_recent_table_data', side_effect=client_error_from_mock_function):
        result = write_to_ingestion_bucket(ur_mdt2, bucket_name, file_location)
        
        assert result.response['Error']['Message'] == error_response['Error']['Message']





def test_function_write_to_ingestion_bucket_raises_correct_exception_if_called_function_update_rows_in_table_fails(S3_setup):
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
        'Error': {
            'Code': 'Error',
            'Message': 'This is a mock ClientError exception raised by mocked version of get_most_recent_table_data().'
                }
                     }
    
    client_error_from_mock_function = ClientError(error_response, "get_object")

    # act and assert: 
    with patch('src.utils_write_to_ingestion_bucket.get_most_recent_table_data', side_effect=client_error_from_mock_function):
        result = write_to_ingestion_bucket(ur_mdt2, bucket_name, file_location)
        
        assert result.response['Error']['Message'] == error_response['Error']['Message']



def test_function_write_to_ingestion_bucket_raises_correct_exception_if_called_function_save_updated_table_to_S3_fails(S3_setup):
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
        'Error': {
            'Code': 'Error',
            'Message': 'This is a mock ClientError exception raised by the mocked version of save_updated_table_to_S3().'
                }
                     }
    
    client_error_from_mock_function = ClientError(error_response, "put_object")

    # act and assert: 
    with patch('src.utils_write_to_ingestion_bucket.save_updated_table_to_S3', side_effect=client_error_from_mock_function):
        result = write_to_ingestion_bucket(ur_mdt2, bucket_name, file_location)
        
        assert result.response['Error']['Message'] == error_response['Error']['Message']







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




