import pytest
import json
import boto3
import os

from moto import mock_aws
from unittest.mock import Mock, patch
from pg8000.native import Connection

from src.first_lambda.first_lambda_handler import first_lambda_handler
from src.first_lambda.first_lambda_utils import get_data_from_db, write_to_s3


# Need to:
# 1) Mock lookup = first_lambda_init()
    # assert called once
    # Mock its return dict
    # Mock that dict's keys' values:
    # bucket_name = lookup['bucket_name'] # name of ingestion bucket
    # tables = lookup['tables'] # list of names of tables of interest
    # s3_client = lookup['s3_client'] # boto3 S3 client object
    # conn = lookup['conn'] # pg8000.native Connection object
    # close_db = lookup['close_db'] # function to close connection to database
# 2) test that change_after_timestamp() gets called once with
#    appropriate args:
#    Mock the function and pass in mocked values for 
#    its args bucket_name, s3_client, "***timestamp***", "1900-01-01 00:00:00".
#    Give the mock a return value of "1900-01-01 00:00:00"    
# 3) test that get_data_from_db(tables, after_time, conn, read_table) is called once.
#    So mock it and pass in mock values for tables, after_time, conn, read_table.
#    Mock its return value data_for_s3 (make up a whole table/list of updated rows)
#    Check it gets called once       
# 4) Mock write_to_s3(data_for_s3, s3_client, write_to_ingestion_bucket, bucket_name)
#    Mock its args (data_for_s3 should already be mocked)
#   Assert the function is called once
# 5) Mock close_db(conn), assert called once with mock conn
# 6) Test that RuntimeError is raised should 
#     get_data_from_db() or write_to_s3() raise Runtime errors.
# 


@pytest.fixture(scope="function")
def general_setup():
    with mock_aws():
        S3_client = boto3.client("s3", region_name="eu-west-2")
        bucket_name_empty = "11-ingestion-bucket_empty"
        bucket_name_with_objs = "11-ingestion-bucket_with_objs"
        # Mock two buckets, one that will be empty
        # one that will contain three tables:
        S3_client.create_bucket(
            Bucket=bucket_name_empty,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )
        S3_client.create_bucket(
            Bucket=bucket_name_with_objs,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
                             )
        
        # values to put in mock dictionary that 
        # mock first_lambda_init() returns 
        # (plus S3_client and bucket_name_empty above):        
        mock_tables = ['design', 'sales_orders', 'transactions'] # list of names of tables of interest
        mock_conn = Mock() # mock pg8000.native.Connection object
        mock_cdb = Mock() # mock close_db
        
        # mock functions passed in to 
        # get_data_from_db() and write_to_s3(),
        # respectively:        
        mock_rt   = Mock() # mock read_table()
        mock_wtib = Mock() # mock write_to_ingestion_bucket()

        mock_fli_return_value = { 
            'bucket_name': bucket_name_empty,
            'tables'     : mock_tables,                     
            's3_client'  : S3_client,  
            'conn'       : mock_conn,
            'close_db'   : mock_cdb
                                 }    
        
        mock_cats_return_value = "1900-01-01 00:00:00"

        mock_gdfd_return_value = [{}, {}, {}]
        



        yield S3_client, 
        bucket_name_empty, 
        bucket_name_with_objs, 
        mock_rt, 
        mock_wtib, 
        mock_fli_return_value, 
        mock_cats_return_value, 
        mock_gdfd_return_value



def test_calls_first_lambda_init_once(general_setup):
    # Arrange:
    (S3_client, 
     bucket_name_empty, 
     bucket_name_with_objs, 
     mock_rt, 
     mock_wtib, 
     mock_fli_return_value,
     mock_cats_return_value,
     mock_gdfd_return_value
     ) = general_setup

    # patch all functions that first_lambda_handler
    # calls without having them passed in:
    with patch('src.first_lambda.first_lambda_handler.first_lambda_init') as fli, \
         patch('src.first_lambda.first_lambda_handler.change_after_time_timestamp') as cats, \
         patch('src.first_lambda.first_lambda_handler.get_data_from_db') as gdfd, \
         patch('src.first_lambda.first_lambda_handler.read_table') as rt, \
         patch('src.first_lambda.first_lambda_handler.write_to_s3') as wts3, \
         patch('src.first_lambda.first_lambda_handler.write_to_ingestion_bucket') as wtib:
        
        fli.return_value  = mock_fli_return_value 
        cats.return_value = mock_cats_return_value
        gdfd.return_value = mock_gdfd_return_value

        # Act:
        first_lambda_handler(None, None)

        # Assert:
        fli.assert_called_once()
        cats.assert_called_once_with(
            mock_fli_return_value['bucket_name'], 
            mock_fli_return_value['s3_client'], 
            "***timestamp***", 
            "1900-01-01 00:00:00"
                                    )

        gdfd.assert_called_once_with(
            mock_fli_return_value['tables'], 
            mock_cats_return_value,
            mock_fli_return_value['conn'],
            rt
            )

        wts3.assert_called_once_with(
            mock_gdfd_return_value,
            mock_fli_return_value['S3_client'],
            wtib,
            mock_fli_return_value['bucket_name'] 
            )
        

        

    