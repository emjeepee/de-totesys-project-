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
from src.utils_write_to_ingestion_bucket import (
    write_to_ingestion_bucket,
    save_updated_table_to_S3,
    get_most_recent_table_data,
)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


tables = [
    "design",
    "payment",
    "sales",
    "transaction",
    "sales_order",
    "counterparty",
    "address",
    "staff",
    "purchase_order",
    "department",
    "currency",
    "payment_type",
]

tables_1 = ["design", "payment"]


@pytest.fixture(scope="function")
def S3_setup(aws_credentials):
    with mock_aws():
        S3_client = boto3.client("s3", region_name="eu-west-2")
        S3_client.create_bucket(
            Bucket="11-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        yield S3_client


# conn = conn_to_db("totesys")
# conn = conn_to_db("")


# @patch("src.conn_to_db.conn_to_db")
def test_get_data_from_db_returns_python_list_each_of_whose_members_is_jsonified_python_list_of_dicts():
    # arrange
    # get_data_from_db returns a python list each of whose members is a jsonified
    # version of this: {'design': [{<data from one row>}, {<data from one row>}, etc]}

    # patch("src.lambda_handler.tables", return_value=["design"])
    mock_dict = {
        "design": [
            {"design_id": 1, "name": "abdul", "team": 1},
            {"design_id": 2, "name": "neil", "team": 1},
            {"design_id": 3, "name": "mukund", "team": 1},
        ]
    }

    # read_table() returns {table_name: [{},{}]}

    mock_read_table = Mock()
    mock_read_table.return_value = mock_dict
    # response = get_data_from_db(tables_1, "1960-01-01 00:00:00", conn, mock_read_table, convert_data)
    response = get_data_from_db(
        tables_1, "1960-01-01 00:00:00", "xxx", mock_read_table, convert_data
    )
    # response should be mock_dict

    # print(f'response is >>> {response}')
    assert json.loads(response[0]) == mock_dict
    # assert json.loads(response[1])["payment"] == mock_dict


# @pytest.mark.skip
def test_write_to_s3_saves_a_whole_table_to_the_bucket_when_no_key_in_bucket_equals_table_name(
    S3_setup,
):
    # s3 bucket is empty
    # write_to_s3 must receive one whole table
    # we want to test that write_to_s3 realises that the S3 bucket
    # contains no keys with prefix eg 'design'
    #
    # arrange:

    table_data = [
        {"design_id": 1, "name": "abdul", "team": 1},
        {"design_id": 2, "name": "neil", "team": 1},
        {"design_id": 3, "name": "mukund", "team": 1},
    ]

    # this is a mock complete table:
    mock_dict = {
        "design": [
            {"design_id": 1, "name": "abdul", "team": 1},
            {"design_id": 2, "name": "neil", "team": 1},
            {"design_id": 3, "name": "mukund", "team": 1},
        ]
    }

    mock_read_table = Mock()
    mock_read_table.return_value = mock_dict

    data_list = get_data_from_db(
        tables_1, "1900-01-01 00:00:00", "xxx", mock_read_table, convert_data
    )

    # data_list should be a list of these kinds of objects jsonified:
    # {'design': [{<data from one row>}, {<data from one row>}, etc]}.
    # in or case data_list will be a list like this [jsonified mock_dict ]
    write_to_s3(data_list, S3_setup, write_to_ingestion_bucket, "11-ingestion-bucket")

    response = S3_setup.list_objects_v2(Bucket="11-ingestion-bucket", Prefix="design")

    assert response["KeyCount"] == 1
    assert response["Prefix"] == "design"


# @pytest.mark.skip
def test_write_to_s3_saves_a_whole_table_to_the_bucket_when_no_key_in_bucket_equals_table_name(
    S3_setup,
):
    # put the whole table in the s3 bucket
    #
    # we want to test that write_to_s3 realises that the S3 bucket
    # does contain a key with prefix eg 'design' and modifies
    # only the rows that this function receives
    #
    # arrange:

    # this is a mock complete table:
    whole_table = [
        {"design_id": 1, "name": "abdul", "team": 1},
        {"design_id": 2, "name": "neil", "team": 1},
        {"design_id": 3, "name": "mukund", "team": 1},
        {"design_id": 4, "name": "Amar", "team": 1},
        {"design_id": 5, "name": "Duncan", "team": 1},
    ]

    # the folllowing is what theis function receives (ie updated rows):
    modified_rows = [
        {"design_id": 1, "name": "abdul", "team": 2},
        {"design_id": 2, "name": "neil", "team": 2},
        {"design_id": 3, "name": "mukund", "team": 2},
    ]

    # put whole in S3 bucket (function save_updated_table_to_S3() has already
    # passed its tests):
    save_updated_table_to_S3(
        json.dumps(whole_table), S3_setup, "design/111111.json", "11-ingestion-bucket"
    )

    mock_read_table = Mock()
    mock_read_table.return_value = {"design": modified_rows}

    data_list = get_data_from_db(
        tables_1, "1900-01-01 00:00:00", "xxx", mock_read_table, convert_data
    )

    # data_list should be a list of these kinds of objects jsonified:
    # {'design': [{<data from one row>}, {<data from one row>}, etc]}.
    # in our case data_list will contain one members like this [jsonified modified_rows ]
    write_to_s3(data_list, S3_setup, write_to_ingestion_bucket, "11-ingestion-bucket")

    # get the most recent table with prefix 'design'
    response = get_most_recent_table_data("design", S3_setup, "11-ingestion-bucket")
    # print(f'response is >>> {response} ')
    # [{'design_id': 1, 'name': 'abdul', 'team': 2}]
    assert response[0]["team"] == 2
    assert response[1]["team"] == 2
    assert response[2]["team"] == 2
