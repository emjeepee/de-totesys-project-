import json
import pytest
from moto import mock_aws
import boto3
from src.lambda_2 import lambda_handler

from src.utils_2 import convert_json_to_python, convert_into_parquet

@pytest.fixture
def mock_bucket():
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        ingestion_bucket = "test-ingestion-bucket"
        processed_bucket = "11-processed-bucket"

        s3_client.create_bucket(Bucket=ingestion_bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"} )
        s3_client.create_bucket(Bucket=processed_bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})

        test_data =[{
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
        }]

        file_key = "sales_order/test_file.json"
        s3_client.put_object(
            Bucket=ingestion_bucket,
            Key=file_key,
            Body=json.dumps(test_data),
        )

        return s3_client, ingestion_bucket, processed_bucket, file_key

@mock_aws
def test_lambda_handler_sales_order(mock_bucket):
    s3_client, ingestion_bucket, processed_bucket, file_key = mock_bucket

    event = {
        "Records": [
        {
            "eventVersion": "2.0",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "1970-01-01T00:00:00.000Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "EXAMPLE"
            },
            "requestParameters": {
                "sourceIPAddress": "127.0.0.1"
            },
            "responseElements": {
                "x-amz-request-id": "EXAMPLE123456789",
                "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "testConfigRule",
                "bucket": {
                    "name": ingestion_bucket,
                    "ownerIdentity": {
                        "principalId": "EXAMPLE"
                    },
                    "arn": "arn:aws:s3:::example-bucket"
                },
                "object": {
                    "key": file_key,
                    "size": 1024,
                    "eTag": "0123456789abcdef0123456789abcdef",
                    "sequencer": "0A1B2C3D4E5F678901"
                }
            }
        }
        ]
    }

    print(ingestion_bucket)
    print(processed_bucket)

    lambda_handler(event, None)


    result = s3_client.list_objects_v2(Bucket=processed_bucket)
    print(result)

