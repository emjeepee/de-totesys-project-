from src.lambda_handler import lambda_handler
from src.change_after_time_timestamp import change_after_time_timestamp
from src.utils import read_table, convert_data
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


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def S3_setup(aws_credentials):
    with mock_aws():
        S3_client = boto3.client("s3", region_name="eu-west-2")
        # Create two buckets. The first will have nothing in it
        # (to allow the testing of function
        # change_after_time_timestamp() when its 'except:' code
        # executes).
        # The second will contain an object for the intital
        # timestamp for the year 1900 (to allow the testing of
        # function change_after_time_timestamp() when its 'try:'
        # code executes:
        test_bckt_1 = "ingestion-bucket-empty"
        test_bckt_2 = "ingestion-bucket-with-initial-1900-timestamp"

        # create the bucket that will contain no timestamp in the test:
        S3_client.create_bucket(
            Bucket=test_bckt_1,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # create the bucket that will contain a timestamp in the test:
        S3_client.create_bucket(
            Bucket=test_bckt_2,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        timesstamp_1900 = "1900-01-01_00-00-00"  # change_after_time_timestamp_stamp()
        # returns this on first ever run of
        # the first lambda, ie this is
        # returned by the 'except:' code (of
        # the function being tested).

        timesstamp_2025 = "2025-06-04_00-00-00"  # change_after_time_timestamp_stamp()
        # returns this when the first lambda
        # runs for the 2nd-plus time, ie this
        # is returned by the 'try:' code (of
        # the function being tested).

        timestamp_key = "***current_timestamp***"

        # put 2025 timestamp into second bucket:
        S3_client.put_object(
            Bucket=test_bckt_2, Key=timestamp_key, Body=timesstamp_2025
        )

        # yield stuff:
        yield_list = [
            S3_client,
            test_bckt_1,
            test_bckt_2,
            timesstamp_1900,
            timesstamp_2025,
            timestamp_key,
        ]
        yield yield_list


class MockConnection:
    def __init__(self, tests: dict[int : dict[object]]):
        """
        test {count} example input:
        {
            {count}:{input:*,time:*,output:*},
            {count}:{input:*,time:*,output:*},
            {count}:{input:*,time:*,output:*}
        }
        """
        self.tests = tests
        self.counter = 0

    def run(self, input: str, after_time=None):
        self.counter += 1
        if self.counter in self.tests:
            return self._run(input, after_time)
        assert False

    def _run(self, input: dict[str:object], after_time=None):
        assert input == self.tests[self.counter]["input"]
        if "time" in self.tests[self.counter]:
            assert after_time == self.tests[self.counter]["time"]
        return self.tests[self.counter]["output"]


@mock_aws
def test_that_lambda_handler_of_first_lambda_function_integrates_all_utilities(
    S3_setup,
):
    yield_list = S3_setup

    # arrange:
    list_of_lists = [
        [1, "abdul", "2025-06-04T00:00:00", "help"],
        [2, "neil", "2025-06-04T00:00:00", "help"],
        [3, "mukkund", "2025-06-04T00:00:00", "help"],
    ]

    mock_dict = {
        "design": [
            {"design_id": 1, "name": "abdul", "team": 1},
            {"design_id": 2, "name": "neil", "team": 1},
            {"design_id": 3, "name": "mukund", "team": 1},
        ]
    }
    columnNames = ["id", "name", "time", "helpme"]
    tables = ["design", "payment"]
    tables = ["design"]  # , "payment" ]
    after_time = change_after_time_timestamp(
        yield_list[1], yield_list[0], yield_list[5], yield_list[3]
    )
    # after_time = "2024-06-04T00:00:00"

    mockConTest1Input = """
        SELECT * FROM design
        WHERE last_updated > :after_time LIMIT 20;
        """
    mockConTest1Time = after_time = after_time
    mockTest1 = {
        "input": mockConTest1Input,
        "time": mockConTest1Time,
        "output": list_of_lists,
    }
    mockConTest2Input = "SELECT column_name FROM information_schema.columns WHERE table_name = 'design' ORDER BY ordinal_position"
    mockTest2 = {"input": mockConTest2Input, "output": columnNames}
    MockCon = MockConnection({1: mockTest1, 2: mockTest2})

    data_for_s3 = get_data_from_db(
        tables, after_time, MockCon, read_table, convert_data
    )

    write_to_s3(data_for_s3, yield_list[0], write_to_ingestion_bucket, yield_list[1])

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
