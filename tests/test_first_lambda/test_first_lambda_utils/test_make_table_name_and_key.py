import pytest
import json
import logging


from unittest.mock import Mock, patch, ANY

from src.first_lambda.first_lambda_utils.make_table_name_and_key import make_table_name_and_key


@pytest.fixture
def mock_values():
    # get_latest_table(resp_dict, S3_client: boto3.client, bucket_name: str):
    yield {
            "Contents": [
                {"Key": 'design/2025-06-02T22-17-19-2513.json'},
                {"Key": 'design/2025-05-29T22-17-19-2513.json'},
                {"Key": 'design/2025-04-29T22-17-19-2513.json'},
                        ]
                    }





def test_make_table_name_and_key_returns_a_list(mock_values):
    # arrange:
    response_dict = mock_values

    # act:
    response = make_table_name_and_key(response_dict)
    result = type(response)
    expected = list

    # assert:
    assert result == expected



def test_make_table_name_and_key_returns_correct_list(mock_values):
    # arrange:
    response_dict = mock_values

    # act:
    response = make_table_name_and_key(response_dict)
    result_0 = response[0]
    result_1 = response[1]
    expected_0 = 'design/2025-06-02T22-17-19-2513.json'
    expected_1 = 'design'
    

    # assert:
    assert result_0 == expected_0
    assert result_1 == expected_1
