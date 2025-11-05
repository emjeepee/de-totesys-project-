import boto3
import pytest
import pandas as pd
import os

from pandas.testing import assert_frame_equal


from moto import mock_aws
from unittest.mock import Mock, patch, call
from datetime import datetime, timedelta

from third_lambda.third_lambda_utils.make_dataframe import make_pandas_dataframe
from src.second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquet



@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

# IMPORTANT!!!!! MAKE A NOTE: Moto persists 
# the bucket state across your tests 
# if fixture S3_setup has scope="module".
# Making the following fixture function-scoped 
# instead of module-scoped means each test gets
# a fresh Moto S3, which means every test runs 
# with a clean S3 environment:
@pytest.fixture(scope="function")
def general_setup():
    with mock_aws():
        mock_S3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name = "11-processed-bucket"
        
        # Mock two buckets, one that will be empty
        # one that will contain three tables:
        mock_S3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )
        
        # object_key = event["Records"][0]["s3"]["object"]["key"]
        # make mock event:
        mock_event = {
                    'Records': [
                    {"s3": 
                     { "object": {
                         'key': "dim_design/2025-06-13_13:23.parquet" # or fact_sales_order/2025-06-13_13-23.parquet
                                 },
                        "bucket": {
                                 "name": "11-processed-bucket",
                                 "ownerIdentity": {
                                 "principalId": "EXAMPLE"
                                                  },
                                  },                                 
                     },
                    }
                              ]
                }
        
        object_key = mock_event['Records'][0]['s3']['object']['key'] # "design/2025-06-13_13:23.parquet"

        dim_des_py = [{'design_id': 1, 'design_name': "Fresh", 'file_location': 'aaaa', 'file_name':'bbbb' },
                      {'design_id': 2, 'design_name': "Stale", 'file_location': 'cccc', 'file_name':'dddd' }, ]
        
        dim_des_pq = convert_to_parquet(dim_des_py)

        # Put the Parquet file
        # in the mock bucket:
        mock_S3_client.put_object( Key=object_key, Bucket=bucket_name, Body=dim_des_pq)


        yield mock_S3_client, bucket_name, object_key, dim_des_py, dim_des_pq





# @pytest.mark.skip
def test_return_value_is_a_dataframe(general_setup):
    # Arrange:
    (mock_S3_client, bucket_name, object_key, dim_des_py, dim_des_pq) = general_setup


    # Act:
    # make_pandas_dataframe(proc_bucket, S3_client, object_key)
    result = make_pandas_dataframe(bucket_name, mock_S3_client, object_key)

    # Assert:
    # assert False
    assert isinstance(result, pd.DataFrame)
    


# @pytest.mark.skip
def test_creates_correct_DataFrame(general_setup):
    # Arrange:
    (mock_S3_client, bucket_name, object_key, dim_des_py, dim_des_pq) = general_setup
    expected = pd.DataFrame(dim_des_py)

    # Act:
    # make_pandas_dataframe(proc_bucket, S3_client, object_key)
    result = make_pandas_dataframe(bucket_name, mock_S3_client, object_key)
    

    # Assert:
    # assert False
    assert_frame_equal(result, expected)

