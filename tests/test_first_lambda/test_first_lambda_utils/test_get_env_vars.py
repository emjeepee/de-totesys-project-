import pytest
import os
import boto3

from botocore.client import BaseClient
from unittest.mock import patch, Mock, ANY

from src.first_lambda.first_lambda_utils.get_env_vars import get_env_vars
from src.first_lambda.first_lambda_utils.conn_to_db import close_db






# @pytest.mark.skip
def test_returns_a_dict():
    # Arrange:
    expected = dict


    # Act:
    response = get_env_vars()
    result = type(response) 

    # Assert:
    assert result == expected





# @pytest.mark.skip
def test_sets_TF_ENVIRONMENT_if_it_does_not_exist():
    # Arrange:
    # Unset TF_ENVIRONMENT 
    # for current process:
    os.environ.pop('TF_ENVIRONMENT', None)
    expected = 'dev'        

    # Act:
    get_env_vars()
    result = os.environ['TF_ENVIRONMENT']

    # Assert:
    assert result == expected





# @pytest.mark.skip
def test_returns_correct_dict_and_env_vars_when_TF_ENVIRONMENT_does_not_exist():
    # Arrange:
    os.environ.pop('TF_ENVIRONMENT', None)

    expected_tables = ['design', 'sales_order', 'counterparty', 'address', 'staff', 'department', 'currency']
    expected_bucket_name = '11-ingestion_bucket'
    # expected_s3_cl_type = 'botocore.client.S3'
    expected_conn_obj = 'conn_obj'

    expected_TF_ENV = 'dev'
    expected_TF_ING_BUCK = '11-ingestion_bucket'
    expected_TF_TBLS_LST = 'design, sales_order, counterparty, address, staff, department, currency'

    # Arrange, act and assert:
    with patch('src.first_lambda.first_lambda_utils.get_env_vars.conn_to_db') as mock_ctdb:
        mock_ctdb.return_value = 'conn_obj'
        response = get_env_vars()
        mock_ctdb.assert_called_once_with('TOTE_SYS')

        result_TF_ENV = os.environ["TF_ENVIRONMENT"]
        result_TF_ING_BUCK = os.environ['TF_INGEST_BUCKET']
        result_TF_TBLS_LST = os.environ['TF_TABLES_LIST']
        
        result_tables = response['tables']
        result_bucket_name = response['bucket_name']
        result_s3_cl = response['s3_client']
        result_conn_obj = response['conn']
        result_cl_db = response['close_db']

        # Assert:
        assert result_TF_ENV == expected_TF_ENV
        assert result_TF_ING_BUCK == expected_TF_ING_BUCK
        assert result_TF_TBLS_LST == expected_TF_TBLS_LST

        assert  result_tables == expected_tables
        assert  result_bucket_name == expected_bucket_name 
        assert isinstance(result_s3_cl, BaseClient)
        assert  result_conn_obj == expected_conn_obj
        assert  result_cl_db is close_db




# @pytest.mark.skip
def test_returns_correct_dict_and_env_vars_when_TF_ENVIRONMENT_does_exist():
    # Arrange:
    os.environ['TF_ENVIRONMENT']    = 'dev'
    os.environ['TF_INGEST_BUCKET']  = '11-ingestion_bucket'
    os.environ['TF_TABLES_LIST']    = 'design, sales_order, counterparty, address, staff, department, currency'



    expected_tables = ['design', 'sales_order', 'counterparty', 'address', 'staff', 'department', 'currency']
    expected_bucket_name = '11-ingestion_bucket'
    # expected_s3_cl_type = 'botocore.client.S3'
    expected_conn_obj = 'conn_obj'

    expected_TF_ENV = 'dev'
    expected_TF_ING_BUCK = '11-ingestion_bucket'
    expected_TF_TBLS_LST = 'design, sales_order, counterparty, address, staff, department, currency'

    # Arrange, act and assert:
    with patch('src.first_lambda.first_lambda_utils.get_env_vars.conn_to_db') as mock_ctdb:
        mock_ctdb.return_value = 'conn_obj'
        response = get_env_vars()
        mock_ctdb.assert_called_once_with('TOTE_SYS')

        result_TF_ENV = os.environ["TF_ENVIRONMENT"]
        result_TF_ING_BUCK = os.environ['TF_INGEST_BUCKET']
        result_TF_TBLS_LST = os.environ['TF_TABLES_LIST']
        
        result_tables = response['tables']
        result_bucket_name = response['bucket_name']
        result_s3_cl = response['s3_client']
        result_conn_obj = response['conn']
        result_cl_db = response['close_db']

        # Assert:
        assert result_TF_ENV == expected_TF_ENV
        assert result_TF_ING_BUCK == expected_TF_ING_BUCK
        assert result_TF_TBLS_LST == expected_TF_TBLS_LST

        assert  result_tables == expected_tables
        assert  result_bucket_name == expected_bucket_name 
        assert isinstance(result_s3_cl, BaseClient)
        assert  result_conn_obj == expected_conn_obj
        assert  result_cl_db is close_db
