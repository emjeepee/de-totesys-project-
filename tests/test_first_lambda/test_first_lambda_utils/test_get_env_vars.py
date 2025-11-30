import pytest
import os

from botocore.client import BaseClient
from unittest.mock import patch 

from src.first_lambda.first_lambda_utils.get_env_vars import get_env_vars
from src.first_lambda.first_lambda_utils.conn_to_db import close_db



# @pytest.mark.skip
def test_returns_a_dict():
    # Arrange:
    expected = dict
    expected_fail = str

    # Act:
    response = get_env_vars()
    result = type(response) 

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected



# @pytest.mark.skip
def test_returns_correct_values_of_env_vars_():
    # Arrange:
    expected_fail = 'fail'
    expected_tables = ['design', 
                       'sales_order', 
                       'counterparty', 
                       'address', 
                       'staff', 
                       'department', 
                       'currency']
    expected_bucket_name = os.environ['AWS_INGEST_BUCKET']
    # expected_s3_cl_type = 'botocore.client.S3'
    expected_conn_obj = 'conn_obj'


    # Act:
    with patch('src.first_lambda.first_lambda_utils.get_env_vars.conn_to_db') as mock_ctdb:
        mock_ctdb.return_value = expected_conn_obj
        response = get_env_vars()
        mock_ctdb.assert_called_once_with('TOTE_SYS')
        
        result_tables = response['tables']
        result_bucket_name = response['bucket_name']
        result_s3_cl = response['s3_client']
        result_conn_obj = response['conn']
        result_close_db = response['close_db']

        # Assert:
        # ensure test can fail:
        # assert result_tables == expected_fail
        assert  result_tables == expected_tables
        assert  result_bucket_name == expected_bucket_name 
        assert isinstance(result_s3_cl, BaseClient)
        assert  result_conn_obj == expected_conn_obj
        assert  result_close_db is close_db

