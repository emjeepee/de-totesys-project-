import json
import pytest
from moto import mock_aws
import boto3
from second_lambda.lambda_2 import lambda_handler
from datetime import datetime
from unittest.mock import patch


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client


class TestSalesOrderIntegration:
    ingestion_bucket = "test-ingestion-bucket"
    processed_bucket = "11-processed-bucket"
    file_key = "sales_order/test_file.json"

    @pytest.fixture(autouse=True)
    def setup(self, s3_client):
        # Create buckets
        s3_client.create_bucket(Bucket=self.ingestion_bucket)
        s3_client.create_bucket(Bucket=self.processed_bucket)

        # Upload sample data
        test_data = [
            {
                "sales_order_id": 1,
                "created_at": "2024-01-01T10:00:00",
                "last_updated": "2024-01-02T10:00:00",
                "staff_id": 1,
                "counterparty_id": 2,
                "units_sold": 10,
                "unit_price": 5.0,
                "currency_id": 1,
                "design_id": 3,
                "agreed_payment_date": "2024-01-15",
                "agreed_delivery_date": "2024-01-20",
                "agreed_delivery_location_id": 4,
            }
        ]

        s3_client.put_object(
            Bucket=self.ingestion_bucket,
            Key=self.file_key,
            Body=json.dumps(test_data),
        )

        self.s3_client = s3_client  # store on the instance

    def test_sales_order_parquet_created(self):
        # Given
        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": self.ingestion_bucket},
                        "object": {"key": self.file_key},
                    }
                }
            ]
        }

        # When
        lambda_handler(event, None)

        # Then
        response = self.s3_client.list_objects_v2(Bucket=self.processed_bucket)
        keys = [item["Key"] for item in response.get("Contents", [])]
        assert any("sales_order.parquet" in key for key in keys)

        # Optional content check
        for key in keys:
            if "sales_order.parquet" in key:
                obj = self.s3_client.get_object(Bucket=self.processed_bucket, Key=key)
                content = obj["Body"].read()
                assert len(content) > 0

    @patch("src.lambda_2.datetime")
    def test_parquet_key_contains_timestamp(self, mock_datetime):
        fake_time = datetime(2025, 1, 1, 12, 0, 0)
        mock_datetime.datetime.now.return_value = fake_time

        def real_datetime(*args, **kwargs):
            return datetime(*args, **kwargs)

        mock_datetime.datetime.side_effect = real_datetime

        event = {
            "Records": [
                {
                    "s3": {
                        "bucket": {"name": self.ingestion_bucket},
                        "object": {"key": self.file_key},
                    }
                }
            ]
        }

        lambda_handler(event, None)

        response = self.s3_client.list_objects_v2(Bucket=self.processed_bucket)
        keys = [item["Key"] for item in response.get("Contents", [])]
        assert any("2025-01-01 12:00:00" in key for key in keys)
