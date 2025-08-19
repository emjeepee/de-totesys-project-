
import boto3
import pytest
import os
from datetime import datetime

from moto import mock_aws
from unittest.mock import Mock, patch, call

from src.second_lambda.second_lambda_utils.second_lambda_init import second_lambda_init


# Need to:
# 1) test second_lambda_init() returns a dict
# 2) test second_lambda_init() returns a dict
#    with the correct keys and values 


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
        bucket_name_empty = "11-ingestion-bucket_empty"
        bucket_name_with_objs = "11-ingestion-bucket_with_objs"
        # Mock two buckets, one that will be empty
        # one that will contain three tables:
        mock_S3_client.create_bucket(
            Bucket=bucket_name_empty,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )
        mock_S3_client.create_bucket(
            Bucket=bucket_name_with_objs,
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
        
        # Create predictable datetime obj
        # including microseconds (because 
        # value of datetime.now() can't 
        # be predicted in tests). 
        mock_dt_now = dt = datetime(2025, 8, 13, 13, 13, 13) # must make timestamp "2025-08-13_13-13-13"

        mock_dt_start = datetime(24, 1, 1) 


        yield mock_event, mock_S3_client, mock_dt_now, mock_dt_start





def test_returns_a_dict(general_setup):
    (mock_event, mock_S3_client, mock_dt_now, mock_dt_start) = general_setup
    # Arrange:
    # second_lambda_init(event, s3_client, dt_now, dt_start)
    expected = dict

    # Act:
    response = second_lambda_init(mock_event, mock_S3_client, mock_dt_now, mock_dt_start)
    # result = None 
    result = type(response) 

    # Assert:
    assert result == expected

    

def test_returns_dict_with_correct_keys_and_values(general_setup):
    (mock_event, mock_S3_client, dt_now, dt_start) = general_setup
    # Arrange:
    # second_lambda_init(event, s3_client, dt_now, dt_start)
    
    expected_keys =   ['s3_client',
                       'timestamp_string',
                       'ingestion_bucket', 
                       'object_key',
                       'proc_bucket', 
                       'table_name', 
                       'start_date']
    
    expected_values = [mock_S3_client,
                       "2025-08-13_13-13-13",
                       "ingestion-bucket",
                       "design/2025-11-11_11:11:11",
                       "11-processed-bucket",
                       'design', 
                       datetime(24, 1, 1)]
    keys = []
    values = []
        # lookup = {
        # 's3_client': s3_client, # boto3 S3 client object
        # 'timestamp_string' : dt_now.strftime("%Y-%m-%d_%H-%M-%S"), # string of format "2025-08-14_12-33-27"
        # 'ingestion_bucket': event["Records"][0]["s3"]["bucket"]["name"], # name of ingestion bucket   
        # 'object_key': object_key, # # key for object in ingestion bucket, eg sales_order/2025-06-04_09-21-32.json
        # 'proc_bucket': "11-processed-bucket", # name of processed bucket:
        # 'table_name': object_key.split("/")[0], # name of table, eg 'sales_order'
        # 'start_date': dt_start # datetime object for 1 Jan 2024 (includes time info - midnight):
        #     }



    # Act:
    response = second_lambda_init(mock_event, mock_S3_client, dt_now, dt_start)

    result_keys = []
    result_values = []
    for key, value in response.items():
        result_keys.append(key)
        result_values.append(value)
        
    # Assert:
    assert result_keys == expected_keys
    assert result_values == expected_values









