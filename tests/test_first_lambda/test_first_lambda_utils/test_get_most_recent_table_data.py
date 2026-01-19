import os
import boto3
import pytest
import json
import logging

from moto import mock_aws
from unittest.mock import Mock, patch, ANY
from datetime import datetime

from botocore.exceptions import ClientError
from src.first_lambda.first_lambda_utils.get_most_recent_table_data import get_most_recent_table_data
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup   


# a pytest fixture runs before 
# each test function that uses it:
@pytest.fixture(scope="function")
def aws_credentials(): # aws_credentials is the name of the fixture
    """Mocked AWS Credentials for moto.
    boto3 reads the following environment 
    variables, so set them:
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"



@pytest.fixture(scope="module")
def test_list():
    
    mock_design_table_1 = [
            {"design_id": 1, "name": "Arty", "team": 11, "project": "ETL"},
            {"design_id": 2, "name": "Beverley", "team": 12, "project": "ETL"},
            {"design_id": 3, "name": "Larry", "team": 13, "project": "ETL"},
                        ]
    
    key = 'design/2025-06-02T22-17-19-2513.json'
    
    mock_json_table = json.dumps(mock_design_table_1)


    mock_values =[mock_json_table, mock_design_table_1, key]
    return mock_values



 # makes fixture available to 
 # all functions in whole module
 # without them needing to call 
 # it in some way:
@pytest.fixture(scope="module")
def S3_setup(test_list):
    """
    Make a mock ingestion bucket that
    contains 
    Here neither S3_client.list_objects_v2()
    nor S3_client.get_object() raises a
    ClientError exception.
    """
    with mock_aws():  # mocks AWS services, comes from library Moto.
        S3_client = boto3.client("s3", region_name="eu-west-2") # make mock S3 client
        bucket_name = "11-ingestion-bucket"
        S3_client.create_bucket(   # make mock bucket
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                               )
        S3_client.put_object(Bucket=bucket_name, 
                             Key=test_list[2], 
                             Body=test_list[0])
        
        # test_list is [mock_json_table, mock_design_table_1, key]
        yield S3_client, bucket_name, test_list[1]







@pytest.fixture(scope="module")
def S3_setup_list_objs_err(S3_setup):
    """
    Fixture that forces S3_client's 
    list_objects_v2() to raise an 
    exception.
    """
    S3_client, bucket_name, design_table = S3_setup

    def mock_list_objects_v2(*args, **kwargs):
        raise ClientError(
            {"Error": {"Code": "500", "Message": "Forcing list_objects_v2 to raise exception"}},
            "ListObjectsV2",
        )
    S3_client.list_objects_v2 = mock_list_objects_v2
    yield_list = [S3_client, bucket_name, design_table]
    yield yield_list





@pytest.fixture(scope="module")
def S3_setup_get_obj_err(S3_setup):
    """
    Fixture that forces S3_client's 
    get_object() to raise an
    exception.
    """
    S3_client, bucket_name, design_table = S3_setup

    def mock_get_object(*args, **kwargs):
        raise ClientError(
            {"Error": {"Code": "500", "Message": "Forcing get_object() to raise exception"}},
            "ListObjectsV2",
                        )
    S3_client.get_object = mock_get_object
    yield S3_client, bucket_name, design_table




# @pytest.mark.skip
def test_get_most_recent_table_data_calls_S3_client_list_objects_v2_functions_correctly(
        S3_setup
        ):
    
    # arrange:    
    S3_client = S3_setup[0]
    bucket_name = S3_setup[1]

    with patch.object(
                    S3_client,
                    "list_objects_v2",
                    wraps=S3_client.list_objects_v2
                     ) as spy: 
                # act:
        most_recent_table = get_most_recent_table_data(
                                                      'design',
                                                       S3_client,
                                                       bucket_name
                                                       )
            
        spy.assert_called_once_with(
                Bucket=bucket_name,
                Prefix='design'
                                   )
            

# @pytest.mark.skip
def test_get_most_recent_table_data_calls_get_latest_table_correctly(
        S3_setup
        ):
    
    # arrange:    
    S3_client = S3_setup[0]
    bucket_name = S3_setup[1]
    
    with patch('src.first_lambda.first_lambda_utils.get_most_recent_table_data.get_latest_table') as mock_glt:
        
        # act:
        most_recent_table = get_most_recent_table_data(
                                                      'design',
                                                       S3_client,
                                                       bucket_name
                                                       )

        # assert: 
        mock_glt.assert_called_once_with(ANY,
                                         S3_client,
                                         bucket_name
                                        )



# @pytest.mark.skip
def test_get_most_recent_table_data_returns_correct_list(S3_setup):
    (
        S3_client,
        bucket_name,
        mock_design_table_1
    ) = S3_setup

    # arrange:
    expected_table = S3_setup[2]

    # act:
    # file_location: str, S3_client: boto3.client, bucket_name: str
    result_table = get_most_recent_table_data('design', S3_setup[0], S3_setup[1])

    # assert:
    assert expected_table == result_table






# @pytest.mark.skip
def test_function_raises_exception_if_attempt_to_list_objects_in_bucket_fails(
                                        S3_setup_list_objs_err
                                                                             ):
    S3_client, bucket_name, mock_design_table_1 = S3_setup_list_objs_err

    with pytest.raises(ClientError):
        get_most_recent_table_data('design', S3_client, bucket_name)





# @pytest.mark.skip
def test_logs_correctly(
    S3_setup_get_obj_err, 
    caplog
                      ):
    (
        S3_client,
        bucket_name,
        mock_design_table_1    
    ) = S3_setup_get_obj_err

    caplog.set_level(logging.ERROR, logger="get_most_recent_table_data")

    with pytest.raises(ClientError):
        get_most_recent_table_data('design', S3_client, bucket_name)

    assert any(errors_lookup['err_4'] in msg for msg in caplog.messages)        



