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

    # Make a mock whole table. A jsonified
    # version would exists in the ingestion 
    # bucket:
        mock_design_table_2 = [
            {"design_id": 1, "name": "Abdul", "team": 1, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 2, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"},
        ]

    # Make a mock list of updated rows.
    # This is what gets passed to 
    # write_to_ingestion_bucket()
        updated_rows_of_mdt2 = [
            {"design_id": 1, "name": "Abdul", "team": 41, "project": "Python"},
            {"design_id": 2, "name": "Mukund", "team": 42, "project": "SQL"},
                               ]
        
    # Make an updated version of 
    # mock_design_table_2:
        mock_dt2_updated = [
            {"design_id": 1, "name": "Abdul", "team": 41, "project": "Python"},
            {"design_id": 2, "name": "Mukund", "team": 42, "project": "SQL"},
            {"design_id": 3, "name": "Neil", "team": 33, "project": "terraform"},
        ]



    # Make a jsonified version of the 
    # mock whole table created above:
        mdt_2_updated_json = json.dumps(mock_dt2_updated)

    # Make a mock key of the type 
    # under which 
    # write_to_ingestion_bucket() would
    # store the updated table to the 
    # ingestion bucket:
        key_mdt2 = "design/2025-06-29T03-57-19-251352.json"

    # The prefix of the key under which
    # write_to_ingestion_bucket() would 
    # store the jsonified updated table 
    # in the injestion bucket:
        file_location = "design"


    # Make a timestamp:
        timestamp = "2025-06-29T03-57-19-251352"         

        yield S3_client, bucket_name, mock_design_table_2, updated_rows_of_mdt2, mock_dt2_updated, mdt_2_updated_json, key_mdt2, file_location, timestamp


# integration testing:
# @pytest.mark.skip
def test_that_funcion_write_to_ingestion_bucket_correctly_integrates_utility_functions(
    S3_setup,
):  
    # Arrange:
    (
        S3_client,
        bucket_name,
        mock_design_table_2,
        updated_rows_of_mdt2,
        mock_dt2_updated,    
        mdt_2_updated_json,
        key_mdt2,
        file_location,
        timestamp
    ) = S3_setup

    # test that write to ingestion bucket 
    # calls these functions with the correct 
    # arguments:
    # get_most_recent_table_data(file_location, s3_client, bucket)
    #           returns: mock_design_table_2 (most recent table)
    # update_rows_in_table(data, latest_table, file_location)
    #           returns: updated version of mock_design_table_2   
    # create_formatted_timestamp()
    #           returns:  "2025-06-29T03-57-19-251352""  
    # save_updated_table_to_S3(updated_table_json, s3_client, new_key, bucket) 
    #           returns: None    

    # Act and Assert:
    with patch('src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.get_most_recent_table_data') as mock_gmrtd, \
         patch('src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.update_rows_in_table') as mock_urit,  \
         patch('src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.create_formatted_timestamp') as mock_cft,  \
         patch('src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.save_updated_table_to_S3') as mock_suttS3:


        mock_gmrtd.return_value = mock_design_table_2
        mock_urit.return_value = mock_dt2_updated
        mock_cft.return_value = timestamp
        
        # write_to_ingestion_bucket(data: list, bucket: str, file_location: str, s3_client: boto3.client):
        write_to_ingestion_bucket( # NOTE: FAILS HERE
                            updated_rows_of_mdt2, 
                            bucket_name, 
                            file_location, 
                            S3_client
                                 )



        # Assert:
        mock_gmrtd.assert_called_once_with(file_location, S3_client, bucket_name) #NOTE: TEST FAILS HERE

        mock_urit.assert_called_once_with(
                                    updated_rows_of_mdt2, 
                                    mock_design_table_2,
                                    file_location
                                         )
        
        mock_cft.assert_called_once()

        mock_suttS3.assert_called_once_with(
            mdt_2_updated_json,
            S3_client,
            key_mdt2,
            bucket_name
                                            )



def test_write_to_ingestion_bucket_raises_RuntimeError_if_get_most_recent_table_data_fails(S3_setup):
    (
        S3_client,
        bucket_name,
        mock_design_table_2,
        updated_rows_of_mdt2,
        mock_dt2_updated,
        mdt_2_json,
        key_mdt2,
        file_location,
        timestamp
    ) = S3_setup

    with patch(
        "src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.get_most_recent_table_data",
        side_effect=RuntimeError("get_most_recent_table_data() failed to read ingestion bucket")
    ):
        with pytest.raises(RuntimeError, match=r"get_most_recent_table_data\(\) failed to read ingestion bucket"):
            write_to_ingestion_bucket(updated_rows_of_mdt2, bucket_name, file_location, S3_client)
            


def test_write_to_ingestion_bucket_raises_RuntimeError_if_save_updated_table_to_S3_fails(S3_setup):
    (
        S3_client,
        bucket_name,
        mock_design_table_2,
        updated_rows_of_mdt2,
        mock_dt2_updated,
        mdt_2_json,
        key_mdt2,
        file_location,
        timestamp
    ) = S3_setup

    with patch(
        "src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.get_most_recent_table_data",
        return_value=mock_design_table_2
    ), patch(
        "src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.update_rows_in_table",
        return_value=mock_dt2_updated
    ), patch(
        "src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.create_formatted_timestamp",
        return_value=timestamp
    ), patch(
        "src.first_lambda.first_lambda_utils.write_to_ingestion_bucket.save_updated_table_to_S3",
        side_effect=RuntimeError("save_updated_table_to_S3() failed to save updated table to ingestion bucket")
    ):
        with pytest.raises(RuntimeError, match=r"save_updated_table_to_S3\(\) failed to save updated table to ingestion bucket"):
            write_to_ingestion_bucket(updated_rows_of_mdt2, bucket_name, file_location, S3_client)            