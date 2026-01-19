import pytest
import json

from unittest.mock import Mock, patch

from src.first_lambda.first_lambda_utils.put_updated_table_in_bucket import put_updated_table_in_bucket



@pytest.fixture(scope="module")
def setup():

       
    mock_table =  {
            'design': [
            {"design_id": 1, "name": "Terry", "team": 42, "project": "terraform"},
            {"design_id": 2, "name": "Barry", "team": 13, "project": "terraform"},
            {"design_id": 3, "name": "Gerry", "team": 3, "project": "terraform"},
                     ]
                        }      

    table_name = 'design'      

    mock_timestamp = 'timestamp'

    mock_s3_client = 'mock_s3_client'

    mock_bucket = 'mock_bucket'

    mock_json_list = json.dumps(mock_table[table_name])

    table_key = f"{table_name}/{mock_timestamp}.json" 

    yield mock_table, table_name, mock_timestamp, mock_s3_client, mock_bucket, mock_json_list, table_key





def test_put_updated_table_in_bucket_calls_internal_function_correctly(setup):
    mock_table, table_name, mock_timestamp, mock_s3_client, mock_bucket, mock_json_list, table_key = setup



    with patch('src.first_lambda.first_lambda_utils.put_updated_table_in_bucket.save_updated_table_to_S3') as mock_sutts3:
        # act:
        put_updated_table_in_bucket(mock_table,
                                    mock_timestamp,
                                    mock_s3_client,
                                    mock_bucket
                                    )

        # assert:
        mock_sutts3.assert_called_once_with(mock_json_list,
                                            mock_s3_client,
                                            table_key,
                                            mock_bucket
                                            )


    