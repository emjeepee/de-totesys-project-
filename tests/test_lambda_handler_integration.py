from src.lambda_handler import lambda_handler
from unittest.mock import Mock, patch, ANY
import pytest
from moto import mock_aws
import json
import boto3
import os
from src.utils import read_table, convert_data
from src.lambda_utils import get_data_from_db, write_to_s3
from src.conn_to_db import conn_to_db
from pg8000.native import Connection
from src.utils_write_to_ingestion_bucket import (
    write_to_ingestion_bucket,
    save_updated_table_to_S3,
    get_most_recent_table_data,
)


def test_that_lambda_handler_of_first_lambda_function_integrates_two_functions():

    # arrange:
    mock_dict = {
        "design": [
            {"design_id": 1, "name": "abdul", "team": 1},
            {"design_id": 2, "name": "neil", "team": 1},
            {"design_id": 3, "name": "mukund", "team": 1},
        ]
    }

    jsonified_mock_dict = json.dumps(mock_dict)

    # with patch("src.conn_to_db.close_db") as mock_close_db, patch("src.conn_to_db.conn_to_db") as mock_conn_to_db, patch("src.lambda_utils.get_data_from_db") as mock_get_data_from_db, patch("src.lambda_utils.write_to_s3") as mock_write_to_s3, patch("src.utils.read_table") as mock_read_table, patch("src.utils.convert_data") as mock_convert_data:
    #     mock_get_data_from_db.return_value = jsonified_mock_dict

    with patch("src.lambda_handler.close_db") as mock_close_db, patch(
        "src.lambda_handler.conn_to_db"
    ) as mock_conn_to_db, patch(
        "src.lambda_handler.get_data_from_db"
    ) as mock_get_data_from_db, patch(
        "src.lambda_handler.write_to_s3"
    ) as mock_write_to_s3, patch(
        "src.lambda_handler.read_table"
    ) as mock_read_table, patch(
        "src.lambda_handler.convert_data"
    ) as mock_convert_data:
        mock_get_data_from_db.return_value = jsonified_mock_dict

        mock_conn_to_db.return_value = "conn"

        lambda_handler(event=None, context=None)

        # (tables, after_time, conn, read_table, convert_data)
        # Check that f1 was called
        mock_get_data_from_db.assert_called_once()

        # Check that f2 was called with the return value of f1
        mock_write_to_s3.assert_called_once_with(
            jsonified_mock_dict, ANY, ANY, "11-ingestion-bucket"
        )
