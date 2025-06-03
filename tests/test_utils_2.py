import boto3.exceptions
from src.utils_2 import *
from unittest.mock import Mock
from moto import mock_aws
import boto3
import pytest
import os
import json


@pytest.fixture(scope="class")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@mock_aws
class TestReadFromS3:
    def test_runs_get_object(self):
        client = boto3.client("s3")
        bucket_name = "test-bucket"
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        key = "test"
        body = "Data"
        upload_to_s3(client, bucket_name, key, body)
        result = read_from_s3(client, bucket_name, key)
        assert result == "Data"

    def test_errors(self):
        client = boto3.client("s3")
        bucket_name = "test"
        key = "test"
        with pytest.raises(Exception) as error:
            read_from_s3(client, bucket_name, key)


@mock_aws
class TestUploadToS3:
    def test_uploads_to_s3(self):
        client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        key = "test"
        body = "Processed data"
        response = upload_to_s3(client, bucket_name, key, body)
        assert response == f"Successfully created {key} in {bucket_name}"

    def test_errors(self):
        client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "test-bucket"
        key = "test"
        body = "Processed data"
        with pytest.raises(Exception) as error:
            upload_to_s3(client, bucket_name, key, body)


class TestJsonConverter:
    def test_converts_json(self):
        test_json = '{"Data_key": "Data_body"}'
        result = convert_json_to_python(test_json)
        assert result == {"Data_key": "Data_body"}

    def test_errors(self):
        test_json = 3
        with pytest.raises(Exception) as error:
            convert_json_to_python(test_json)

@mock_aws
class TestTransformToStarSchema:
    def test_transform_star_schema(self):
        input = [
            {   "sales_order_id": 1,
                "created_at": "2025-05-03T12:00:00",
                "last_updated":"2025-06-01T12:23:36",
                "staff_id": 1,
                "counterparty_id": 1,
                "units_sold": 10,
                "unit_price": 300.00,
                "currency_id": 1,
                "design_id": 5,
                "agreed_payment_date": "2025-05-03",
                "agreed_delivery_date": "2025-05-25",
                "agreed_delivery_location_id": 14
                }
            ]
        expected = [
            {   "sales_order_id": 1,
                "created_date": "2025-05-03",
                "created_time": "12:00:00",
                "last_updated_date": "2025-06-01",
                "last_updated_time": "12:23:36",
                "sales_staff_id": 1,
                "counterparty_id": 1,
                "units_sold": 10,
                "unit_price": 300.00,
                "currency_id": 1,
                "design_id": 5,
                "agreed_payment_date": "2025-05-03",
                "agreed_delivery_date": "2025-05-25",
                "agreed_delivery_location_id": 14
                }
            ]
        result = transform_to_star_schema_fact_table("sales_order", input)
        assert result == expected
