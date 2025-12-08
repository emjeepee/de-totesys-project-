import boto3
import pytest
import json
import os
import logging

from moto import mock_aws
from unittest.mock import Mock, patch, call
from datetime import datetime

from botocore.exceptions import ClientError



from src.first_lambda.first_lambda_utils.write_tables_to_ing_buck import write_tables_to_ing_buck
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup

# What to do:
# 1) test that the function calls create_formatted_timestamp() once
#           (✔︎)


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
# That was the earlier setting and it cause 
# weird things to happen, specifically the 
# mock create_formatted_timestamp() got 
# put into the empty bucket!!!!!!
# Making the following fixture function-scoped 
# instead of module-scoped means each test gets
# a fresh Moto S3, which means every test runs 
# with a clean S3 environment:
@pytest.fixture(scope="function")
def S3_setup():
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



        # Make a mock list that contains 
        # three dictionaries, each representing
        # updated rows for one table:
        mock_data_list_updated_rows = [
                {
            'design': 
            [{"design_id": 1, "name": "Abdul", "team": 1, "project": "updated_SQL"},
            {"design_id": 2, "name": "Mukund", "team": 2, "project": "updated_terraform"}
            ] 
                },
                            {
            'sales_orders': 
            [
            {"sales_orders_id": 25, "name": "Mukund", "team": 5, "project": "updated_terraform"},
            {"sales_orders_id": 26, "name": "updated_name", "team": 6, "project": "terraform"},]
                },
                {
            'transactions': 
            [{"transactions_id": 17, "name": "Abdul", "team": 7, "project": "updated_SQL"},
             {"transactions_id": 19, "name": "updated_name", "team": 9, "project": "terraform"}]
                }

                                     ]

        mock_data_list_whole_tables = [
                {
            'design': 
            [{"design_id": 1, "name": "Abdul", "team": 1, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 2, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"},]
                },
                            {
            'sales_orders': 
            [{"sales_orders_id": 24, "name": "Abdul", "team": 4, "project": "terraform"},
            {"sales_orders_id": 25, "name": "Mukund", "team": 5, "project": "terraform"},
            {"sales_orders_id": 26, "name": "Neil", "team": 6, "project": "terraform"},]
                },
                {
            'transactions': 
            [{"transactions_id": 17, "name": "Abdul", "team": 7, "project": "terraform"},
            {"transactions_id": 18, "name": "Mukund", "team": 8, "project": "terraform"},
            {"transactions_id": 19, "name": "Neil", "team": 9, "project": "terraform"},]
                }

                                     ]
        
        
        de_key =       'design/2025-06-11_13-27-29.json'
        so_key = 'sales_orders/2025-06-11_13-27-29.json'
        tr_key = 'transactions/2025-06-11_13-27-29.json'

        de_wt_body = json.dumps(mock_data_list_whole_tables[0]['design'])
        so_wt_body = json.dumps(mock_data_list_whole_tables[1]['sales_orders'])
        tr_wt_body = json.dumps(mock_data_list_whole_tables[2]['transactions'])

        # put the tables in the bucket:
        S3_client.put_object( Bucket=bucket_name_with_objs, Body = de_wt_body, Key= de_key )
        S3_client.put_object( Bucket=bucket_name_with_objs, Body= so_wt_body, Key= so_key )
        S3_client.put_object( Bucket=bucket_name_with_objs, Body= tr_wt_body, Key= tr_key )        



        yield S3_client, bucket_name_empty, bucket_name_with_objs, mock_data_list_updated_rows, mock_data_list_whole_tables, de_key, so_key, tr_key, de_wt_body, so_wt_body, tr_wt_body



# def test_modified_rows_go_into_S3_bucket(S3_setup):
    
#     # Arrange:
#     (
#      S3_client, 
#      bucket_name_empty,
#      bucket_name_with_objs,  # this is of interest in this test
#      mock_wtib, 
#      mock_data_list_updated_rows,  # this is of interest in this test
#      mock_data_list_whole_tables,
#      de_key, 
#      so_key, 
#      tr_key
#      ) = S3_setup
    
#     with patch('src.first_lambda.first_lambda_utils.write_to_s3.create_formatted_timestamp') as mock_cft:

#         # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
#         write_to_s3(mock_data_list_updated_rows, S3_client, write_to_ingestion_bucket, bucket_name_with_objs)

#         # Get the keys that are NOT de_key, so_key or tr_key
#         # (there will only be one for each table, so 3 altogether):
#         de_list = S3_client.list_objects_v2(Bucket = bucket_name_with_objs, Prefix = 'design')['Contents'] # a list
#         de_key_1 = sorted([de_list[0]['Key'], de_list[1]['Key']], reverse=True)[0] # design/2025-08-18_19-22-59.json

#         so_list = S3_client.list_objects_v2(Bucket = bucket_name_with_objs, Prefix = 'sales_orders')['Contents'] # a list
#         so_key_1 = sorted([so_list[0]['Key'], so_list[1]['Key']], reverse=True)[0]

#         tr_list = S3_client.list_objects_v2(Bucket = bucket_name_with_objs, Prefix = 'transactions')['Contents'] # a list
#         tr_key_1 = sorted([tr_list[0]['Key'], tr_list[1]['Key']], reverse=True)[0]

#         # Get the updated tables:
#         rxed_de_table = json.loads((S3_client.get_object(Bucket = bucket_name_with_objs, Key = de_key_1))['Body'].read().decode("utf-8"))
#         rxed_so_table = json.loads((S3_client.get_object(Bucket = bucket_name_with_objs, Key = so_key_1))['Body'].read().decode("utf-8"))
#         rxed_tr_table = json.loads((S3_client.get_object(Bucket = bucket_name_with_objs, Key = tr_key_1))['Body'].read().decode("utf-8"))

#         expected_de_row_0 = {"design_id": 1, "name": "Abdul", "team": 1, "project": "updated_SQL"}
#         expected_de_row_1 = {"design_id": 2, "name": "Mukund", "team": 2, "project": "updated_terraform"}
#         expected_de_row_2 = {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"}

#         expected_so_row_0 = {"sales_orders_id": 24, "name": "Abdul", "team": 4, "project": "terraform"}
#         expected_so_row_1 = {"sales_orders_id": 25, "name": "Mukund", "team": 5, "project": "updated_terraform"}
#         expected_so_row_2 = {"sales_orders_id": 26, "name": "updated_name", "team": 6, "project": "terraform"}

#         expected_tr_row_0 = {"transactions_id": 17, "name": "Abdul", "team": 7, "project": "updated_SQL"}
#         expected_tr_row_1 = {"transactions_id": 18, "name": "Mukund", "team": 8, "project": "terraform"}
#         expected_tr_row_2 = {"transactions_id": 19, "name": "updated_name", "team": 9, "project": "terraform"}

       
#         result_de_row_0 = rxed_de_table[0]
#         result_de_row_1 = rxed_de_table[1]
#         result_de_row_2 = rxed_de_table[2]

#         result_so_row_0 = rxed_so_table[0]
#         result_so_row_1 = rxed_so_table[1]
#         result_so_row_2 = rxed_so_table[2]

#         result_tr_row_0 = rxed_tr_table[0]
#         result_tr_row_1 = rxed_tr_table[1]
#         result_tr_row_2 = rxed_tr_table[2]

#         assert result_de_row_0 == expected_de_row_0
#         assert result_de_row_1 == expected_de_row_1
#         assert result_de_row_2 == expected_de_row_2

#         assert result_so_row_0 == expected_so_row_0
#         assert result_so_row_1 == expected_so_row_1
#         assert result_so_row_2 == expected_so_row_2

#         assert result_tr_row_0 == expected_tr_row_0
#         assert result_tr_row_1 == expected_tr_row_1
#         assert result_tr_row_2 == expected_tr_row_2





# @pytest.mark.skip
def test_calls_create_formatted_timestamp_once(S3_setup):
    # Arrange:
    (
     S3_client, 
     bucket_name_empty,  
     bucket_name_with_objs,  
     mock_data_list_updated_rows, # not important which table used
     mock_data_list_whole_tables,
     de_key, 
     so_key, 
     tr_key,
     de_wt_body,
     so_wt_body, 
     tr_wt_body
     ) = S3_setup
    

    with patch('src.first_lambda.first_lambda_utils.write_tables_to_ing_buck.create_formatted_timestamp') as mock_cft:

        # write_tables_to_ing_buck(s3_client, bucket_name, data_list ):
        write_tables_to_ing_buck(S3_client, bucket_name_empty, mock_data_list_updated_rows)

        mock_cft.assert_called_once()




# @pytest.mark.skip
def test_calls_save_updated_table_to_s3_correctly(S3_setup):
    # Arrange:
    (
     S3_client, 
     bucket_name_empty,  
     bucket_name_with_objs,  
     mock_data_list_updated_rows, # not important which table used
     mock_data_list_whole_tables,
     de_key, 
     so_key, 
     tr_key,
     de_wt_body,
     so_wt_body, 
     tr_wt_body
     ) = S3_setup
    
    ts_1 = "design/ts.json"
    ts_2 = "sales_orders/ts.json"
    ts_3 = "transactions/ts.json"
    

    with patch('src.first_lambda.first_lambda_utils.write_tables_to_ing_buck.create_formatted_timestamp') as mock_cft, \
        patch('src.first_lambda.first_lambda_utils.write_tables_to_ing_buck.save_updated_table_to_S3') as mock_sutts3:
        mock_cft.return_value = 'ts'
        mock_sutts3.return_value = None

        write_tables_to_ing_buck(S3_client, bucket_name_empty, mock_data_list_whole_tables)

        # assert:
        # called exactly 3 times
        assert mock_sutts3.call_count == 3
        
        # called with correct 
        # arguments each time:
        mock_sutts3.assert_has_calls(
            [     
                call(de_wt_body, S3_client, ts_1, bucket_name_empty),
                call(so_wt_body, S3_client, ts_2, bucket_name_empty),
                call(tr_wt_body, S3_client, ts_3, bucket_name_empty),
            ]
        )










# If the ingestion bucket is empty,
# that means this is the first ever
# run of the first lambda function
# and data_list must contain whole
# tables, so: 
# i)   use the empty mock S3 bucket.
# ii)  set data_list to contain whole 
#      tables. 
# iii) mock create_formatted_timestamp()
#      so that it returns a known 
#      mock timestamp. 
# iv)  test that the bucket ends up
#      containing those whole tables. 
# @pytest.mark.skip
# def test_correct_rows_go_into_bucket_when_bucket_is_empty(S3_setup):
#     # Arrange:
#     (
#      S3_client, 
#      bucket_name_empty, # of interest in this test
#      bucket_name_with_objs,  
#      mock_wtib,  # not needed 
#      mock_data_list_updated_rows, # not needed 
#      mock_data_list_whole_tables, # of interest in this test
#      de_key,  # not needed 
#      so_key,  # not needed 
#      tr_key   # not needed 
#      ) = S3_setup
    

#     with patch('src.first_lambda.first_lambda_utils.write_to_s3.create_formatted_timestamp') as mock_cft:
#         mock_cft.return_value = "1111-11-11_11-11-11"    
#         # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
#         write_to_s3(mock_data_list_whole_tables, S3_client, write_to_ingestion_bucket, bucket_name_empty)

#         key_for_de = 'design/'       + "1111-11-11_11-11-11.json"
#         key_for_so = 'sales_orders/' + "1111-11-11_11-11-11.json"
#         key_for_tr = 'transactions/' + "1111-11-11_11-11-11.json"


#         # get the tables saved in the bucket:
#         rxed_de_table = json.loads((S3_client.get_object(Bucket = bucket_name_empty, Key = key_for_de))['Body'].read().decode("utf-8"))
#         rxed_so_table = json.loads((S3_client.get_object(Bucket = bucket_name_empty, Key = key_for_so))['Body'].read().decode("utf-8"))
#         rxed_tr_table = json.loads((S3_client.get_object(Bucket = bucket_name_empty, Key = key_for_tr))['Body'].read().decode("utf-8"))

#         expected_de_row_0 = mock_data_list_whole_tables[0]['design'][0]
#         expected_de_row_1 = mock_data_list_whole_tables[0]['design'][1]
#         expected_de_row_2 = mock_data_list_whole_tables[0]['design'][2]

#         expected_so_row_0 = mock_data_list_whole_tables[1]['sales_orders'][0]
#         expected_so_row_1 = mock_data_list_whole_tables[1]['sales_orders'][1]
#         expected_so_row_2 = mock_data_list_whole_tables[1]['sales_orders'][2]

#         expected_tr_row_0 = mock_data_list_whole_tables[2]['transactions'][0]
#         expected_tr_row_1 = mock_data_list_whole_tables[2]['transactions'][1]
#         expected_tr_row_2 = mock_data_list_whole_tables[2]['transactions'][2]

#         result_de_row_0 = rxed_de_table[0]
#         result_de_row_1 = rxed_de_table[1]
#         result_de_row_2 = rxed_de_table[2]

#         result_so_row_0 = rxed_so_table[0]
#         result_so_row_1 = rxed_so_table[1]
#         result_so_row_2 = rxed_so_table[2]

#         result_tr_row_0 = rxed_tr_table[0]
#         result_tr_row_1 = rxed_tr_table[1]
#         result_tr_row_2 = rxed_tr_table[2]

#         assert result_de_row_0 == expected_de_row_0
#         assert result_de_row_1 == expected_de_row_1
#         assert result_de_row_2 == expected_de_row_2

#         assert result_so_row_0 == expected_so_row_0
#         assert result_so_row_1 == expected_so_row_1
#         assert result_so_row_2 == expected_so_row_2

#         assert result_tr_row_0 == expected_tr_row_0
#         assert result_tr_row_1 == expected_tr_row_1
#         assert result_tr_row_2 == expected_tr_row_2



# Need to test the function
# logs an exception when
# s3_client.list_objects_v2()
# fails (in the first try 
# block):
# @pytest.mark.skip
# def test_logs_exception_when_first_try_block_code_fails(S3_setup, caplog):
#         # Arrange:
#     (
#      S3_client, 
#      bucket_name_empty, 
#      bucket_name_with_objs,  
#      mock_wtib,  
#      mock_data_list_updated_rows, 
#      mock_data_list_whole_tables, 
#      de_key,
#      so_key,
#      tr_key 
#      ) = S3_setup
    
#     mock_s3_client = Mock()
#     mock_s3_client.list_objects_v2.side_effect = ClientError(
#     {"Error": {"Code": "500", "Message": 
#                "Failed to list objects in ingestion bucket"}
#         },
#      "ListObjectsV2"
#                                                            ) 

    
#     caplog.set_level(logging.ERROR, logger="write_to_s3")

#     with pytest.raises(ClientError):
#         write_to_s3(mock_data_list_whole_tables, mock_s3_client, mock_wtib, bucket_name_empty)
#         # NOTE: code here (ie that 
#         # comes after the line above, 
#         # which raises the exception, 
#         # will not run!

#     expected_err_msg = errors_lookup['err_7a']
#     assert any(expected_err_msg in msg for msg in caplog.messages)
        
    




# Need to test the function
# logs an exception when
# s3_client.put_object()
# fails (in the second try 
# block):
# @pytest.mark.skip
# def test_logs_exception_when_second_try_block_code_fails(S3_setup, caplog):
#         # Arrange:
#     (
#      S3_client, 
#      bucket_name_empty, 
#      bucket_name_with_objs,  
#      mock_wtib,  
#      mock_data_list_updated_rows, 
#      mock_data_list_whole_tables, 
#      de_key,
#      so_key,
#      tr_key 
#      ) = S3_setup
    
#     mock_s3_client = Mock()
#     # the following line ensures 
#     # that the else statement 
#     # in write_to_s3() runs:
#     mock_s3_client.list_objects_v2.return_value = {"KeyCount": 0}
#     mock_s3_client.put_object.side_effect = ClientError(
#     {"Error": {"Code": "500", "Message": 
#                "Failed to list objects in ingestion bucket"}
#         },
#      "ListObjectsV2"
#                                                            ) 

    
#     caplog.set_level(logging.ERROR, logger="write_to_s3")

#     with pytest.raises(ClientError):
#         write_to_s3(mock_data_list_whole_tables, mock_s3_client, mock_wtib, bucket_name_empty)
#         # NOTE: code here (ie that 
#         # comes after the line above, 
#         # which raises the exception, 
#         # will not run!

#     expected_err_msg = errors_lookup['err_7b']
#     assert any(expected_err_msg in msg for msg in caplog.messages)