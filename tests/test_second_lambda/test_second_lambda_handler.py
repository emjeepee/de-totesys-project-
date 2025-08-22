import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import calendar
import pytest
import os
import json
import boto3

from moto import mock_aws
from unittest import Mock, patch
from io import BytesIO
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

from src.second_lambda.second_lambda_handler import second_lambda_handler



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
        # mock_S3_client = boto3.client("s3", region_name="eu-west-2")
        mock_S3_client = boto3.client
        b_name_1 = "11-ingestion-bucket"
        b_name_2 = "11-processed-bucket"
        
        # Create a mock ingestion bucket:
        mock_S3_client.create_bucket( # empty bucket
            Bucket=b_name_1,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )
        
        # Create a mock processed bucket:
        mock_S3_client.create_bucket( # empty bucket
            Bucket=b_name_2,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )
        

        mock_event =     {
        "Records": [
          {        
            "s3": {
                "bucket": {
                    "name": "ingestion-bucket",
                          },
                "object": {
                    "key": "design/2025-11-11_11:11:11", # the key under which the object has been saved 
                          }
                  }
          }
                  ]
                }   




        yield mock_S3_client, b_name_1, b_name_2, mock_event








# @pytest.mark.skip
def test_integration_of_utility_functions(general_setup):
    # Arrange:
    (mock_S3_client, b_name_1, b_name_2, mock_event) = general_setup
    

    # Act and assert:
    # lookup = second_lambda_init(event, boto3.client("s3"), datetime.now(), datetime(2024, 1, 1))
    # table_json = read_from_s3(s3_client, ingestion_bucket, object_key)
    # if is_first_run_of_pipeline(proc_bucket, s3_client):
        # arr = create_dim_date_Parquet(start_date, timestamp_string, num_rows)
        # upload_to_s3(s3_client, proc_bucket, arr[1], arr[0])
    # dim_or_fact_table = make_dim_or_fact_table(table_name, table_python, s3_client, ingestion_bucket)
    # pq_file = convert_to_parquet(dim_or_fact_table)
    # upload_to_s3(s3_client, proc_bucket, table_key, pq_file)

    with patch('src.second_lambda.second_lambda_handler.second_lambda_init') as mock_sli, \
         patch('src.second_lambda.second_lambda_handler.read_from_s3') as mock_rfs3, \
         patch('src.second_lambda.second_lambda_handler.is_first_run_of_pipeline') as mock_ifrop, \
         patch('src.second_lambda.second_lambda_handler.boto3.client') as mock_S3_client:
            second_lambda_handler(mock_event, 'context')
            mock_sli.assert_called_once_with(mock_event, mock_S3_client("s3"), datetime.now(), datetime(2024, 1, 1))









        # sli_return_dict = {
        #  's3_client': mock_S3_client,               # boto3 S3 client object,
        #  'ingestion_bucket': b_name_1,              # name of bucket,
        #  'object_key': '',                          # ingestion bucket stores object under this key
        #  'table_name': 'design',                    # name of (in this case dimension) table
        #  'proc_bucket': b_name_2,                   # name of processed bucket
        #  'start_date': datetime(24, 1, 1),          # start date from which code makes date dimension table
        #  'num_rows': 3                              # number of rows (ie days) date dimension table covers
        #                   } 



# 
        
        # # make a test key, which will in real code
        # # look like:
        # # "design/2025-06-13_13:23:34.parquet" 
        # # OR 
        # # fact_sales_order/2025-06-13_13:23:34.parquet 
        # test_key = "design/2025-06-13_13:23:34.parquet"

        # # create a test prefix:
        # test_prefix = "design"

        # test_body = json.dumps([{'test_1': 1}, {'test_2': 2}, {'test_3': 3}])
