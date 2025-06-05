import boto3.exceptions
from src.utils_2 import *
from unittest.mock import Mock
from moto import mock_aws
import boto3
import pytest
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO


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
            {
                "sales_order_id": 1,
                "created_at": "2025-05-03T12:00:00",
                "last_updated": "2025-06-01T12:23:36",
                "staff_id": 1,
                "counterparty_id": 1,
                "units_sold": 10,
                "unit_price": 300.00,
                "currency_id": 1,
                "design_id": 5,
                "agreed_payment_date": "2025-05-03",
                "agreed_delivery_date": "2025-05-25",
                "agreed_delivery_location_id": 14,
            }
        ]
        expected = [
            {
                "sales_order_id": 1,
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
                "agreed_delivery_location_id": 14,
            }
        ]
        result = transform_to_star_schema_fact_table("sales_order", input)
        assert result == expected

    def test_transform_empty_list(self):
        input = []
        expected = []
        result = transform_to_star_schema_fact_table("sales_order", input)
        assert result == expected

    def test_transform_wrong_table_name(self):
        input = [{"id": "1", "first_name": "john", "surname": "python"}]
        expected = []
        result = transform_to_star_schema_fact_table("coders", input)
        assert result == expected

    def test_transform_exception(self):

        input = [
            {
                "sales_order_id": 1,
                "created_at": "Yesterday",
                "last_updated": "2025-06-01T12:23:36",
                "staff_id": 1,
                "counterparty_id": 1,
                "units_sold": 10,
                "unit_price": 300.00,
                "currency_id": 1,
                "design_id": 5,
                "agreed_payment_date": "2025-05-03",
                "agreed_delivery_date": "2025-05-25",
                "agreed_delivery_location_id": 14,
            }
        ]

        with pytest.raises(Exception) as error:
            transform_to_star_schema_fact_table("sales_order", input)


class TestDimStaffTransform:
    def test_dim_staff(self):
        input = [
            {
                "staff_id": "1",
                "first_name": "john",
                "last_name": "python",
                "email_address": "john@python.org",
                "department_id": "2",
            }
        ]
        department = [
            {"department_id": "2", "department_name": "coding", "location": "there"}
        ]

        expected = [
            {
                "staff_id": "1",
                "first_name": "john",
                "last_name": "python",
                "email_address": "john@python.org",
                "department_name": "coding",
                "location": "there",
            }
        ]

        result = transform_to_dim_staff(input, department)

        assert expected == result

    def test_dim_staff_exception(self):
        input = [
            {
                "staff_id": "1",
                "first_name": "john",
                "last_name": "python",
                "email_address": "john@python.org",
                "department_id": "4",
            }
        ]
        department = [
            {"department_id": "2", "department_name": "coding", "location": "there"}
        ]

        with pytest.raises(Exception) as error:
            transform_to_dim_staff(input, department)


class TestDimLocationTransform:
    def test_location_transforms_data(self):
        input = [
            {
                "address_id": "1",
                "address_line_1": "42 Python Way",
                "address_line_2": "Terraform",
                "district": "System21",
                "city": "windows",
                "postal_code": "WIN 21",
                "country": "SSD",
                "phone": "07777777777",
                "created_at": "2025-03-01T10:00:00",
                "last_updated": "2025-04-02T11:00:00",
            }
        ]

        expected = [
            {
                "location_id": "1",
                "address_line_1": "42 Python Way",
                "address_line_2": "Terraform",
                "district": "System21",
                "city": "windows",
                "postal_code": "WIN 21",
                "country": "SSD",
                "phone": "07777777777",
            }
        ]

        result = transform_to_dim_location(input)

        assert result == expected

    def test_location_raises_exception(self):
        input = {
            "address_id": "1",
            "address_line_1": "42 Python Way",
            "address_line_2": "Terraform",
            "district": "System21",
            "city": "windows",
            "postal_code": "WIN 21",
            "country": "SSD",
            "phone": "07777777777",
            "created_at": "2025-03-01T10:00:00",
            "last_updated": "2025-04-02T11:00:00",
        }

        with pytest.raises(Exception) as error:
            transform_to_dim_location(input)


class TestDimDesignTransform:
    def test_design_transforms_data(self):
        input = [
            {
                "design_id": "1",
                "design_name": "Snake",
                "file_location": "src/file",
                "file_name": "System21",
                "created_at": "2025-03-01T10:00:00",
                "last_updated": "2025-04-02T11:00:00",
            }
        ]

        expected = [
            {
                "design_id": "1",
                "design_name": "Snake",
                "file_location": "src/file",
                "file_name": "System21",
            }
        ]

        result = transform_to_dim_design(input)

        assert result == expected

    def test_design_raises_exception(self):
        input = {
            "design_id": "1",
            "design_name": "Snake",
            "file_location": "src/file",
            "file_name": "System21",
            "created_at": "2025-03-01T10:00:00",
            "last_updated": "2025-04-02T11:00:00",
        }

        with pytest.raises(Exception) as error:
            transform_to_dim_design(input)


class TestDimCounterpartyTransform:
    def test_counterparty_transforms_data(self):
        counterparty = [
            {
                "counterparty_id": 1,
                "counterparty_legal_name": "legal name",
                "legal_address_id": 3,
                "commercial_contact": "person name",
                "delivery_contact": "person name II",
                "created_at": "2025-03-01T10:00:00",
                "last_updated": "2025-04-02T11:00:00",
            }
        ]

        address = [
            {
                "address_id": 3,
                "address_line_1": "line 1",
                "address_line_2": "line 2",
                "district": "district",
                "city": "city",
                "postal_code": "postal_code",
                "country": "country",
                "phone": "phone",
            }
        ]

        expected = [
            {
                "counterparty_id": 1,
                "counterparty_legal_name": "legal name",
                "counterparty_legal_address_line_1": "line 1",
                "counterparty_legal_address_line_2": "line 2",
                "counterparty_legal_district": "district",
                "counterparty_legal_city": "city",
                "counterparty_legal_postal_code": "postal_code",
                "counterparty_legal_country": "country",
                "counterparty_legal_phone_number": "phone",
            }
        ]

        result = transform_to_dim_counterparty(counterparty, address)

        assert result == expected

    def test_counterparty_raises_exception(self):
        counterparty = [
            {
                "counterparty_id": 1,
                "counterparty_legal_name": "legal name",
                "_legal_address_id": 3,
                "commercial_contact": "person name",
                "delivery_contact": "person name II",
                "created_at": "2025-03-01T10:00:00",
                "last_updated": "2025-04-02T11:00:00",
            }
        ]

        address = [
            {
                "address_id": 2,
                "address_line_1": "line 1",
                "address_line_2": "line 2",
                "district": "district",
                "city": "city",
                "postal_code": "postal_code",
                "country": "country",
                "phone": "phone",
            }
        ]

        with pytest.raises(Exception) as error:
            transform_to_dim_counterparty(counterparty, address)


class TestDimCurrencyTransform:
    def test_transforms_data(self):
        input = [
            {
                "currency_id": "1",
                "currency_code": "CAD",
                "created_at": "2025-03-01T10:00:00",
                "last_updated": "2025-04-02T11:00:00",
            }
        ]

        expected = [
            {
                "currency_id": "1",
                "currency_code": "CAD",
                "currency_name": "Canadian Dollar",
            }
        ]

        result = transform_to_dim_currency(input)

        assert result == expected

    def test_raises_exception(self):
        input = {
            "currency_id": "1",
            "currency_code": "CAD",
            "created_at": "2025-03-01T10:00:00",
            "last_updated": "2025-04-02T11:00:00",
        }

        with pytest.raises(Exception) as error:
            transform_to_dim_currency(input)


class TestDimDateTransform:
    def test_date_counts_rows(self):
        result = transform_to_dim_date("2025-06-01", "2025-06-03")
        assert len(result) == 3

    def test_date_has_keys(self):
        result = transform_to_dim_date("2025-06-01", "2025-06-03")
        expected = {
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        }

        assert result[0].keys() == expected

    def test_date_id_counts_up(self):
        result = transform_to_dim_date("2025-06-01", "2025-06-03")
        assert [row["date_id"] for row in result] == [1, 2, 3]

    def test_date_raises_error(self):
        with pytest.raises(Exception):
            transform_to_dim_date("2025-06-05", "2025-06-02")


class TestConvertToParquet:
    def test_converted_to_parquet(self):
        data = [
            {
                "design_id": "1",
                "design_name": "Snake",
                "file_location": "src/file",
                "file_name": "System21",
            }
        ]
        result = convert_into_parquet(data)

        assert isinstance(
            pd.read_parquet(result, engine="pyarrow"), pd.core.frame.DataFrame
        )

    def test_parquet_conversion_error(self):
        data = "yes"

        with pytest.raises(Exception):
            convert_into_parquet(data)
