import pytest
import json
import logging


from botocore.exceptions import ClientError
from unittest.mock import Mock, patch, ANY

from src.first_lambda.first_lambda_utils.get_latest_table import get_latest_table
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup



@pytest.fixture
def mock_values():
    # get_latest_table(resp_dict, S3_client: boto3.client, bucket_name: str):
    mock_resp_dict = {
            "Contents": [
                {"Key": 'design/2025-06-02T22-17-19-2513.json'},
                {"Key": 'design/2025-05-29T22-17-19-2513.json'},
                {"Key": 'design/2025-04-29T22-17-19-2513.json'},
                        ]
                    }

    mock_s3_client = 'mock_s3_client'

    mock_bucket_name = "mock_ingestion-bucket"

    yield_list = [mock_resp_dict,
                  mock_s3_client,
                  mock_bucket_name
                 ]

    yield yield_list




# @pytest.mark.skip
def test_get_latest_table_calls_internal_functions_correctly(mock_values):
    # Arrange
    mock_vals        = mock_values
    mock_resp_dict   = mock_vals[0]
    mock_s3_client   = mock_vals[1]
    mock_bucket_name = mock_vals[2]
    


    with patch('src.first_lambda.first_lambda_utils.get_latest_table.make_table_name_and_key') as mock_mtnak, \
        patch('src.first_lambda.first_lambda_utils.get_latest_table.retrieve_latest_table') as mock_rlt:
        mock_mtnak.return_value = ['latest_table_key','table_name']
        mock_rlt.return_value = 'latest_table'
        
        # act:
        result = get_latest_table(mock_resp_dict,
                                  mock_s3_client,
                                  mock_bucket_name
                                  )

        # assert:
        assert result == 'latest_table'
        mock_mtnak.assert_called_once_with(mock_resp_dict)
        mock_rlt.assert_called_once_with(mock_s3_client,
                                         mock_bucket_name,
                                         'latest_table_key',
                                         'table_name'
                                             )

        
        


        



