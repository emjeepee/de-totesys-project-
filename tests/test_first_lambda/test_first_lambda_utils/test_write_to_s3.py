from moto import mock_aws
import boto3
import pytest
import json
from unittest.mock import Mock, patch, call
from datetime import datetime
import os
from botocore.exceptions import ClientError


from src.first_lambda.first_lambda_utils.write_to_s3 import write_to_s3
from src.first_lambda.first_lambda_utils.write_to_ingestion_bucket import write_to_ingestion_bucket


# What to do:
# 1) test that the function calls create_formatted_timestamp() once
#           (✔︎)
# 2) test that the function calls write_to_ingestion_bucket() 
#           the correct number of times (because it's in a loop)
#           (✔︎)    
# 4) in one test function test that write_to_s3 works 
#           for the case where data_list contains only updated rows
#           and there are already tables in the mock S3 bucket. Put those
#           tables in the mock bucket first
# 5) in another test function test that write_to_s3 works 
#           for the case where data_list contains whole tables and
#           there is nothing in the mock S3 bucket. Ensure the 
#           bucket is empty.
# 3) test that the function raise RuntimeErrors for
#           s3_client.list_objects_v2()
#           write_to_ingestion_bucket()
#           s3_client.put_object        
# 6) create mocks of data_list, s3_client, write_to_ingestion_bucket(), bucket_name
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

        # mock write_to_ingestion_bucket()
        mock_wtib = Mock()


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

        de_body = json.dumps(mock_data_list_whole_tables[0]['design'])
        so_body = json.dumps(mock_data_list_whole_tables[1]['sales_orders'])
        tr_body = json.dumps(mock_data_list_whole_tables[2]['transactions'])

        S3_client.put_object( Bucket=bucket_name_with_objs, Body = de_body, Key= de_key )
        S3_client.put_object( Bucket=bucket_name_with_objs, Body= so_body, Key= so_key )
        S3_client.put_object( Bucket=bucket_name_with_objs, Body= tr_body, Key= tr_key )        



        yield S3_client, bucket_name_empty, bucket_name_with_objs, mock_wtib, mock_data_list_updated_rows, mock_data_list_whole_tables, de_key, so_key, tr_key








def test_calls_write_to_ingestion_bucket_correct_number_of_times(S3_setup):
    
    # Arrange:
    (
     S3_client, 
     bucket_name_empty,
     bucket_name_with_objs,  # this is of interest in this test
     mock_wtib, 
     mock_data_list_updated_rows,  # this is of interest in this test
     mock_data_list_whole_tables,
     de_key, 
     so_key, 
     tr_key
     ) = S3_setup
    
    key_design, value_design             = next(iter(mock_data_list_updated_rows[0].items())) # 'design' and [{...}, {...}, {...}, etc] 
    key_sales_order, value_sales_order   = next(iter(mock_data_list_updated_rows[1].items())) # 'sales_order' and [{...}, {...}, {...}, etc] 
    key_transactions, value_transactions = next(iter(mock_data_list_updated_rows[2].items())) # 'transactions' and [{...}, {...}, {...}, etc]     

    # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
    write_to_s3(mock_data_list_updated_rows, S3_client, mock_wtib, bucket_name_with_objs)

    assert mock_wtib.call_count == 3

    # Assert the arguments for each call (in order)
    expected_calls = [
            # write_to_ingestion_bucket(data: list, bucket: str, file_location: str, s3_client: boto3.client)
            call(value_design, bucket_name_with_objs, key_design, S3_client),
            call(value_sales_order, bucket_name_with_objs, key_sales_order, S3_client),
            call(value_transactions, bucket_name_with_objs, key_transactions, S3_client),
                     ]

    mock_wtib.assert_has_calls(expected_calls, any_order=False)






def test_modified_rows_go_into_S3_bucket(S3_setup):
    
    # Arrange:
    (
     S3_client, 
     bucket_name_empty,
     bucket_name_with_objs,  # this is of interest in this test
     mock_wtib, 
     mock_data_list_updated_rows,  # this is of interest in this test
     mock_data_list_whole_tables,
     de_key, 
     so_key, 
     tr_key
     ) = S3_setup
    
    with patch('src.first_lambda.first_lambda_utils.write_to_s3.create_formatted_timestamp') as mock_cft:

        # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
        write_to_s3(mock_data_list_updated_rows, S3_client, write_to_ingestion_bucket, bucket_name_with_objs)

        # Get the keys that are NOT de_key, so_key or tr_key
        # (there will only be one for each table, so 3 altogether):
        de_list = S3_client.list_objects_v2(Bucket = bucket_name_with_objs, Prefix = 'design')['Contents'] # a list
        de_key_1 = sorted([de_list[0]['Key'], de_list[1]['Key']], reverse=True)[0] # design/2025-08-18_19-22-59.json

        so_list = S3_client.list_objects_v2(Bucket = bucket_name_with_objs, Prefix = 'sales_orders')['Contents'] # a list
        so_key_1 = sorted([so_list[0]['Key'], so_list[1]['Key']], reverse=True)[0]

        tr_list = S3_client.list_objects_v2(Bucket = bucket_name_with_objs, Prefix = 'transactions')['Contents'] # a list
        tr_key_1 = sorted([tr_list[0]['Key'], tr_list[1]['Key']], reverse=True)[0]

        # Get the updated tables:
        rxed_de_table = json.loads((S3_client.get_object(Bucket = bucket_name_with_objs, Key = de_key_1))['Body'].read().decode("utf-8"))
        rxed_so_table = json.loads((S3_client.get_object(Bucket = bucket_name_with_objs, Key = so_key_1))['Body'].read().decode("utf-8"))
        rxed_tr_table = json.loads((S3_client.get_object(Bucket = bucket_name_with_objs, Key = tr_key_1))['Body'].read().decode("utf-8"))

        expected_de_row_0 = {"design_id": 1, "name": "Abdul", "team": 1, "project": "updated_SQL"}
        expected_de_row_1 = {"design_id": 2, "name": "Mukund", "team": 2, "project": "updated_terraform"}
        expected_de_row_2 = {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"}

        expected_so_row_0 = {"sales_orders_id": 24, "name": "Abdul", "team": 4, "project": "terraform"}
        expected_so_row_1 = {"sales_orders_id": 25, "name": "Mukund", "team": 5, "project": "updated_terraform"}
        expected_so_row_2 = {"sales_orders_id": 26, "name": "updated_name", "team": 6, "project": "terraform"}

        expected_tr_row_0 = {"transactions_id": 17, "name": "Abdul", "team": 7, "project": "updated_SQL"}
        expected_tr_row_1 = {"transactions_id": 18, "name": "Mukund", "team": 8, "project": "terraform"}
        expected_tr_row_2 = {"transactions_id": 19, "name": "updated_name", "team": 9, "project": "terraform"}

       
        result_de_row_0 = rxed_de_table[0]
        result_de_row_1 = rxed_de_table[1]
        result_de_row_2 = rxed_de_table[2]

        result_so_row_0 = rxed_so_table[0]
        result_so_row_1 = rxed_so_table[1]
        result_so_row_2 = rxed_so_table[2]

        result_tr_row_0 = rxed_tr_table[0]
        result_tr_row_1 = rxed_tr_table[1]
        result_tr_row_2 = rxed_tr_table[2]

        assert result_de_row_0 == expected_de_row_0
        assert result_de_row_1 == expected_de_row_1
        assert result_de_row_2 == expected_de_row_2

        assert result_so_row_0 == expected_so_row_0
        assert result_so_row_1 == expected_so_row_1
        assert result_so_row_2 == expected_so_row_2

        assert result_tr_row_0 == expected_tr_row_0
        assert result_tr_row_1 == expected_tr_row_1
        assert result_tr_row_2 == expected_tr_row_2





# @pytest.mark.skip
def test_calls_create_formatted_timestamp_once(S3_setup):
    # Arrange:
    (
     S3_client, 
     bucket_name_empty,  # use of empty S3 bucket means mock_wtib will not be called
     bucket_name_with_objs,  
     mock_wtib, 
     mock_data_list_updated_rows, # not important which table used
     mock_data_list_whole_tables,
     de_key, 
     so_key, 
     tr_key
     ) = S3_setup
    

    with patch('src.first_lambda.first_lambda_utils.write_to_s3.create_formatted_timestamp') as mock_cft:

        # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
        write_to_s3(mock_data_list_updated_rows, S3_client, 'mock_wtib', bucket_name_empty)

        mock_cft.assert_called_once()



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
def test_correct_rows_go_into_bucket_when_bucket_is_empty(S3_setup):
    # Arrange:
    (
     S3_client, 
     bucket_name_empty, # of interest in this test
     bucket_name_with_objs,  
     mock_wtib,  # not needed 
     mock_data_list_updated_rows, # not needed 
     mock_data_list_whole_tables, # of interest in this test
     de_key,  # not needed 
     so_key,  # not needed 
     tr_key   # not needed 
     ) = S3_setup
    

    with patch('src.first_lambda.first_lambda_utils.write_to_s3.create_formatted_timestamp') as mock_cft:
        mock_cft.return_value = "1111-11-11_11-11-11"    
        # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
        write_to_s3(mock_data_list_whole_tables, S3_client, write_to_ingestion_bucket, bucket_name_empty)

        key_for_de = 'design/'       + "1111-11-11_11-11-11.json"
        key_for_so = 'sales_orders/' + "1111-11-11_11-11-11.json"
        key_for_tr = 'transactions/' + "1111-11-11_11-11-11.json"


        # get the tables saved in the bucket:
        rxed_de_table = json.loads((S3_client.get_object(Bucket = bucket_name_empty, Key = key_for_de))['Body'].read().decode("utf-8"))
        rxed_so_table = json.loads((S3_client.get_object(Bucket = bucket_name_empty, Key = key_for_so))['Body'].read().decode("utf-8"))
        rxed_tr_table = json.loads((S3_client.get_object(Bucket = bucket_name_empty, Key = key_for_tr))['Body'].read().decode("utf-8"))

        expected_de_row_0 = mock_data_list_whole_tables[0]['design'][0]
        expected_de_row_1 = mock_data_list_whole_tables[0]['design'][1]
        expected_de_row_2 = mock_data_list_whole_tables[0]['design'][2]

        expected_so_row_0 = mock_data_list_whole_tables[1]['sales_orders'][0]
        expected_so_row_1 = mock_data_list_whole_tables[1]['sales_orders'][1]
        expected_so_row_2 = mock_data_list_whole_tables[1]['sales_orders'][2]

        expected_tr_row_0 = mock_data_list_whole_tables[2]['transactions'][0]
        expected_tr_row_1 = mock_data_list_whole_tables[2]['transactions'][1]
        expected_tr_row_2 = mock_data_list_whole_tables[2]['transactions'][2]

        result_de_row_0 = rxed_de_table[0]
        result_de_row_1 = rxed_de_table[1]
        result_de_row_2 = rxed_de_table[2]

        result_so_row_0 = rxed_so_table[0]
        result_so_row_1 = rxed_so_table[1]
        result_so_row_2 = rxed_so_table[2]

        result_tr_row_0 = rxed_tr_table[0]
        result_tr_row_1 = rxed_tr_table[1]
        result_tr_row_2 = rxed_tr_table[2]

        assert result_de_row_0 == expected_de_row_0
        assert result_de_row_1 == expected_de_row_1
        assert result_de_row_2 == expected_de_row_2

        assert result_so_row_0 == expected_so_row_0
        assert result_so_row_1 == expected_so_row_1
        assert result_so_row_2 == expected_so_row_2

        assert result_tr_row_0 == expected_tr_row_0
        assert result_tr_row_1 == expected_tr_row_1
        assert result_tr_row_2 == expected_tr_row_2



# Need to test the generation
# of a RuntimeError by 
# s3_client.list_objects_v2()
# in the first try statement:
# @pytest.mark.skip
def test_raises_RuntimeError_when_fails_to_list_objects_in_bucket(S3_setup):
        # Arrange:
    (
     S3_client, 
     bucket_name_empty, 
     bucket_name_with_objs,  
     mock_wtib,  
     mock_data_list_updated_rows, 
     mock_data_list_whole_tables, 
     de_key,  # not needed 
     so_key,  # not needed 
     tr_key   # not needed 
     ) = S3_setup

    S3_client.list_objects_v2 = Mock(side_effect=ClientError(
    {"Error": {"Code": "500", "Message": "Failed to list objects in bucket"}},
    "ListObjectsV2"
                                        ))
    
    with pytest.raises(RuntimeError):
        # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
        write_to_s3(mock_data_list_whole_tables,  S3_client,  mock_wtib, bucket_name_empty)

    



# Need to test the generation
# of a RuntimeError by 
# write_to_ingestion_bucket(),
# which wrtite_to_s3() calls 
# in the second try-except block:
# @pytest.mark.skip
def test_raises_RuntimeError_when_write_to_ingestion_bucket_fails(S3_setup):
        # Arrange:
    (
     S3_client, 
     bucket_name_empty, 
     bucket_name_with_objs,  
     mock_wtib,  
     mock_data_list_updated_rows, 
     mock_data_list_whole_tables, 
     de_key,  # not needed 
     so_key,  # not needed 
     tr_key   # not needed 
     ) = S3_setup

    mock_wtib.side_effect=RuntimeError()
    
    with pytest.raises(RuntimeError):
        # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
        write_to_s3(mock_data_list_updated_rows,  S3_client,  mock_wtib, bucket_name_with_objs)

    


# Need to test the generation
# of a RuntimeError by 
# S3_client.put_object() in 
# third try-except block:
# @pytest.mark.skip
def test_S3_client_put_object_raises_RuntimeError_when_it_fails(S3_setup):
        # Arrange:
    (
     S3_client, 
     bucket_name_empty, 
     bucket_name_with_objs,  
     mock_wtib,  
     mock_data_list_updated_rows, 
     mock_data_list_whole_tables, 
     de_key,  # not needed 
     so_key,  # not needed 
     tr_key   # not needed 
     ) = S3_setup

    S3_client.put_object = Mock(side_effect=ClientError(
    {"Error": {"Code": "500", "Message": "Failed to put objects in bucket"}},
    "PutObject"
                                        ))

    
    with pytest.raises(RuntimeError):
        # write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
        write_to_s3(mock_data_list_whole_tables,  S3_client,  mock_wtib, bucket_name_empty)

    









    # response_design       = S3_client.list_objects_v2(Bucket=bucket_name_with_objs, Prefix='design')
    # response_sales_orders = S3_client.list_objects_v2(Bucket=bucket_name_with_objs, Prefix='sales_orders')
    # response_transactions = S3_client.list_objects_v2(Bucket=bucket_name_with_objs, Prefix='transactions')


    # if response_design["KeyCount"] > 0:
    #     print(f'There are {response_design["KeyCount"]} objects with prefix -design- in the bucket')            
    #     print(f'The name is {response_design['Contents'][0]['Key']}')
    # if response_sales_orders["KeyCount"] > 0:
    #     print(f'There are {response_sales_orders["KeyCount"]} objects with prefix -sales_orders- in the bucket')            
    #     print(f'The name is {response_sales_orders['Contents'][0]['Key']}')
    # if response_transactions["KeyCount"] > 0:
    #     print(f'There are {response_transactions["KeyCount"]} objects with prefix -transactions- in the bucket')            
    #     print(f'The name is {response_transactions['Contents'][0]['Key']}') 


        # de_list = S3_client.list_objects_v2(Bucket = bucket_name_empty, Prefix = 'design')['Contents'] # a list
        # print(f'de_list is {de_list}')
        # # de_key_1 = sorted([de_list[0]['Key'], de_list[1]['Key']], reverse=True)[0] # design/2025-08-18_19-22-59.json

        # so_list = S3_client.list_objects_v2(Bucket = bucket_name_empty, Prefix = 'sales_orders')['Contents'] # a list
        # print(f'so_list is {so_list}')
        # # so_key_1 = sorted([so_list[0]['Key'], so_list[1]['Key']], reverse=True)[0]

        # tr_list = S3_client.list_objects_v2(Bucket = bucket_name_empty, Prefix = 'transactions')['Contents'] # a list
        # print(f'tr_list is {tr_list}')