import pytest
import os

from botocore.client import BaseClient
from unittest.mock import patch, Mock 

from src.first_lambda.first_lambda_utils.get_env_vars import get_env_vars
from src.first_lambda.first_lambda_utils.conn_to_db import close_db





def test_returns_a_dict():
    # Arrange:
    expected = dict

    # Act:
    response = get_env_vars()
    result = type(response) 

    # Assert:
    assert result == expected




def test_get_env_vars_returns_expected_dictionary(monkeypatch):

    # Arrange:
    # Make mock environment variables
    monkeypatch.setenv("AWS_TABLES_LIST", "table_name_1, table_name_2, table_name_3")
    monkeypatch.setenv("AWS_INGEST_BUCKET", "mock_ingest_bucket")
    monkeypatch.setenv("AWS_PROCESS_BUCKET", "mock_process_bucket")
    monkeypatch.setenv("OLTP_NAME", "mock_db_name")

    # Mock the boto3.client
    mock_s3_client = Mock()

    mock_returned_tables_list = ["table_name_1", "table_name_2", "table_name_3"]

    with patch("src.first_lambda.first_lambda_utils.get_env_vars.boto3.client") as mock_boto:
        mock_boto.return_value = mock_s3_client

        # Mock database functions
        mock_conn = Mock(name="mock_db_connection")
        mock_close_db = Mock(name="mock_close_db")

        with patch("src.first_lambda.first_lambda_utils.get_env_vars.conn_to_db") as mock_conn_to_db, \
            patch("src.first_lambda.first_lambda_utils.get_env_vars.close_db") as mock_close_db:
            mock_conn_to_db.return_value=mock_conn

            # act:
            lookup = get_env_vars()

    # assert:
    assert lookup["tables"] == mock_returned_tables_list
    assert lookup["ing_bucket_name"] == "mock_ingest_bucket"
    assert lookup["proc_bucket_name"] == "mock_process_bucket"

    assert lookup["s3_client"] is mock_s3_client
    assert lookup["conn"] is mock_conn
    assert lookup["close_db"] is mock_close_db

    mock_boto.assert_called_once_with("s3")
    mock_conn_to_db.assert_called_once_with("mock_db_name")
