from src.lambda_handler import lambda_handler
from unittest.mock import Mock, patch
import pytest
from moto import mock_aws
import json
import boto3
import os
from src.utils import read_table, convert_data
from src.lambda_utils import get_data_from_db, write_to_s3
from src.conn_to_db import conn_to_db
from pg8000.native import Connection




@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

tables = ["design", "payment", "sales", "transaction", "sales_order", "counterparty", "address", "staff", "purchase_order", "department", "currency", "payment_type" ]

tables_1 = ["design","payment"]

@pytest.fixture(scope="function")
def S3_setup(aws_credentials):
    with mock_aws():
        S3_client = boto3.client("s3", region_name="eu-west-2")
        S3_client.create_bucket(
            Bucket="11-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        
        yield S3_client

conn = conn_to_db("totesys")

# @patch("src.conn_to_db.conn_to_db")
def test_get_data_from_db_returns_python_list_each_of_whose_members_is_jsonified_python_list_of_dicts():
    # arrange
   
    # patch("src.lambda_handler.tables", return_value=["design"])
    value_to_be_returned = [{"design_id": 1, "name": "abdul", "team":1},
                            {"design_id": 2, "name": "neil", "team":1},
                            {"design_id": 3, "name": "mukund", "team":1}]
    with patch("pg8000.native.Connection.run", return_value=value_to_be_returned):
        response = get_data_from_db(tables_1, "1960-01-01 00:00:00",conn, read_table,convert_data)
        # Response is a python list each of whose memebers is a jsonified python list of dictionaries 
        print(response)
        assert json.loads(response[0])["design"] == value_to_be_returned
        assert json.loads(response[1])["payment"] == value_to_be_returned



def test_write_to_s3_calls_write_to_ingestion(S3_setup):
    table_data = [{"design_id": 1, "name": "abdul", "team":1},
                            {"design_id": 2, "name": "neil", "team":1},
                            {"design_id": 3, "name": "mukund", "team":1}]
    
    data_list = get_data_from_db(tables_1, "1900-01-01 00:00:00", conn, read_table, convert_data)

    write_to_s3(data_list)

    response =  S3_setup.list_objects_v2(Bucket="11-ingestion-bucket", Key=tables_1[0])

    assert response["KeyCount"] == 1
    assert response["Prefix"] == "design"




