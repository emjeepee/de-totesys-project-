import pytest
import os
import json
import boto3
import logging

from moto import mock_aws
from unittest.mock import patch, ANY, call, Mock
from datetime import datetime 
from botocore.exceptions import ClientError

from src.second_lambda.second_lambda_handler import second_lambda_handler
from src.second_lambda.second_lambda_utils.info_lookup import info_lookup
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
        # mock_S3_client = boto3.client("s3", region_name="eu-west-2")
        mock_boto3_client = boto3.client
        b_name_1 = "11-ingestion-bucket"
        b_name_2 = "11-processed-bucket"
        
        mock_S3_client = mock_boto3_client("s3", region_name="eu-west-2")

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
                    "name": "11-ingestion-bucket",
                          },
                "object": {
                    "key": "design/2025-11-11_11:11:11", # the key under which the object has been saved 
                          }
                  }
          }
                  ]
                }   

        mock_py_tbl = [
             {'design_id': 1, 
              'created_at': '2025-8-12-12-11-10-73000)', 
              'design_name': 'Fresh',
              'file_location': '/Network',
              'file_name': 'fresh-20240124-ap0b.json', 
              'last_updated': '2025-8-12-12-11-10-73000'
              },
                {'design_id': 2, 
              'created_at': '2025-8-12-12-11-10-73000)', 
              'design_name': 'Stale',
              'file_location': '/Network',
              'file_name': 'Stale-20240124-ap0b.json', 
              'last_updated': '2025-8-12-12-11-10-73000)'
              }
                     ]

        mock_json_tbl = json.dumps(mock_py_tbl)

        # make mock design dimension table:
        mock_mdoft_return = [
             {'design_id': 1, 'design_name': 'Fresh', 'file_location': 'aaa', 'file_name': 'bbb'},
             {'design_id': 2, 'design_name': 'Stale', 'file_location': 'ccc', 'file_name': 'ddd'}
                            ]
        
        mock_pq = convert_to_parquet(mock_mdoft_return, 'design')

        object_key = mock_event["Records"][0]["s3"]["object"]["key"]

        mock_sli_return = {
        's3_client': mock_S3_client, # boto3 S3 client object
        'timestamp_string' : "2025-08-22_15-30-00", # string of format "2025-08-14_12-33-27"
        'ingestion_bucket': mock_event["Records"][0]["s3"]["bucket"]["name"], # name of ingestion bucket   
        'object_key': object_key, # # key for object in ingestion bucket, eg sales_order/2025-06-04_09-21-32.json
        'proc_bucket': "11-processed-bucket", # name of processed bucket:
        'table_name': object_key.split("/")[0], # name of table, eg 'sales_order'
        'start_date': datetime(24, 1, 1), # datetime object for 1 Jan 2024 (includes time info for midnight)
        'num_rows' : 3 # a datetime object for 1 Jan 2024
                            }
        
        mock_ts = "dim_design/2025-08-22_15-30-00.parquet"


        mock_sli_return_1 = {
        's3_client': mock_S3_client, # boto3 S3 client object
        'timestamp_string' : "2025-08-22_15-30-00", # string of format "2025-08-14_12-33-27"
        'ingestion_bucket': mock_event["Records"][0]["s3"]["bucket"]["name"], # name of ingestion bucket   
        'object_key': object_key, # # key for object in ingestion bucket, eg sales_order/2025-06-04_09-21-32.json
        'proc_bucket': "11-processed-bucket", # name of processed bucket:
        'table_name': 'department', # name of table, eg 'sales_order'
        'start_date': datetime(24, 1, 1), # datetime object for 1 Jan 2024 (includes time info for midnight)
        'num_rows' : 3 # a datetime object for 1 Jan 2024
                            }


        yield (mock_event, 
               mock_py_tbl, 
               mock_json_tbl, 
               mock_mdoft_return, 
               mock_pq, 
               mock_sli_return, 
               mock_ts, 
               mock_sli_return_1) 








# @pytest.mark.skip
def test_integrates_correct_utility_functions_when_first_run_of_pipeline(general_setup):
    """
    this tests the correct integration 
    of functions when utility function 
    is_first_run_of_pipeline() returns 
    True
    """
    # Arrange:
    (mock_event, 
     mock_py_tbl, 
     mock_json_tbl, 
     mock_mdoft_return, 
     mock_pq, 
     mock_sli_return, 
     mock_ts,
     mock_sli_return_1  ) = general_setup
    

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
        patch('src.second_lambda.second_lambda_handler.create_dim_date_Parquet') as mock_cddP, \
        patch('src.second_lambda.second_lambda_handler.create_formatted_timestamp') as mock_cfts, \
        patch('src.second_lambda.second_lambda_handler.upload_to_s3') as mock_uts3, \
        patch('src.second_lambda.second_lambda_handler.make_dim_or_fact_table') as mock_mdoft, \
        patch('src.second_lambda.second_lambda_handler.convert_to_parquet') as mock_ctp, \
        patch('src.second_lambda.second_lambda_handler.make_dim_or_fact_table_keystring') as mock_mdoftk:
              
            mock_sli.return_value = mock_sli_return
            mock_rfs3.return_value = mock_json_tbl
            mock_ifrop.return_value = True
            mock_cddP.return_value = 'mock_pq_table_in_buff'
            mock_cfts.return_value = 'mock_timestamp'
            mock_mdoft.return_value = mock_mdoft_return
            mock_ctp.return_value = mock_pq
            mock_mdoftk.return_value = 'mock_dim_or_fact_keystring'
            

            # Act:
            second_lambda_handler(mock_event, 'context')
            mock_sli.assert_called_once_with(mock_event, 
                                             ANY, 
                                             ANY, 
                                             ANY, 
                                             2557)
            
            # read_from_s3(s3_client from lookup, ingestion_bucket from lookup, object_key from lookup)
            mock_rfs3.assert_called_once_with(mock_sli_return['s3_client'],  
                                              mock_sli_return['ingestion_bucket'],  
                                              mock_sli_return['object_key'] )
            
            # is_first_run_of_pipeline(lookup['proc_bucket'],  lookup['s3_client'])
            mock_ifrop.assert_called_once_with(mock_sli_return['proc_bucket'], 
                                               mock_sli_return['s3_client'] )
            
            # create_dim_date_Parquet(lookup['start_date'], lookup['timestamp_string'], lookup['num_rows'])
            mock_cddP.assert_called_once_with(mock_sli_return['start_date'],
                                              mock_sli_return['timestamp_string'],
                                              mock_sli_return['num_rows'])
            
            mock_cfts.assert_called_once()

            # make_dim_or_fact_table(table_name--from lookup, table_python, s3_client--from lookup, ingestion_bucket--from lookup)
            mock_mdoft.assert_called_once_with(mock_sli_return['table_name'], 
                                               mock_py_tbl, 
                                               mock_sli_return['s3_client'], 
                                               mock_sli_return['ingestion_bucket'])

            # convert_to_parquet(dim_or_fact_table--return of mdoft)
            mock_ctp.assert_called_once_with(mock_mdoft_return, 
                                             mock_sli_return['table_name'])
            
            mock_mdoftk.assert_called_once_with(mock_sli_return['table_name'],
                                                mock_sli_return['timestamp_string'])

            # upload_to_s3(s3_client, proc_bucket, table_key, pq_file)
            assert mock_uts3.call_count == 2

            # 2️⃣ Assert the exact calls in order
            mock_uts3.assert_has_calls(
            [
                call(mock_sli_return['s3_client'], 
                     mock_sli_return['proc_bucket'], 
                     "dim_date/mock_timestamp.parquet", 
                     'mock_pq_table_in_buff'),

                call(mock_sli_return['s3_client'], 
                     mock_sli_return['proc_bucket'],  
                     'mock_dim_or_fact_keystring', 
                     mock_pq),
            ],
            any_order=False
                                    )




# @pytest.mark.skip
def test_integrates_correct_utility_functions_when_2nd_plus_run_of_pipeline(general_setup):
    """
    this tests the correct integration 
    of functions when utility function 
    is_first_run_of_pipeline() returns 
    False
    """
    # Arrange:
    (mock_event, 
     mock_py_tbl, 
     mock_json_tbl, 
     mock_mdoft_return, 
     mock_pq, 
     mock_sli_return, 
     mock_ts,
     mock_sli_return_1  ) = general_setup
    

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
        patch('src.second_lambda.second_lambda_handler.create_dim_date_Parquet') as mock_cddP, \
        patch('src.second_lambda.second_lambda_handler.create_formatted_timestamp') as mock_cfts, \
        patch('src.second_lambda.second_lambda_handler.upload_to_s3') as mock_uts3, \
        patch('src.second_lambda.second_lambda_handler.make_dim_or_fact_table') as mock_mdoft, \
        patch('src.second_lambda.second_lambda_handler.convert_to_parquet') as mock_ctp, \
        patch('src.second_lambda.second_lambda_handler.make_dim_or_fact_table_keystring') as mock_mdoftk:
              
            mock_sli.return_value = mock_sli_return
            mock_rfs3.return_value = mock_json_tbl
            mock_ifrop.return_value = False
            mock_cddP.return_value = 'mock_pq_table_in_buff'
            mock_cfts.return_value = 'mock_timestamp'
            mock_mdoft.return_value = mock_mdoft_return
            mock_ctp.return_value = mock_pq
            mock_mdoftk.return_value = 'mock_dim_or_fact_keystring'
            

            # Act:
            second_lambda_handler(mock_event, 'context')
            mock_sli.assert_called_once_with(mock_event, 
                                             ANY, 
                                             ANY, 
                                             ANY, 
                                             2557)
            
            # read_from_s3(s3_client from lookup, ingestion_bucket from lookup, object_key from lookup)
            mock_rfs3.assert_called_once_with(mock_sli_return['s3_client'],  
                                              mock_sli_return['ingestion_bucket'],  
                                              mock_sli_return['object_key'] )
            
            # is_first_run_of_pipeline(lookup['proc_bucket'],  lookup['s3_client'])
            mock_ifrop.assert_called_once_with(mock_sli_return['proc_bucket'], 
                                               mock_sli_return['s3_client'] )
            

            # make_dim_or_fact_table(table_name--from lookup, table_python, s3_client--from lookup, ingestion_bucket--from lookup)
            mock_mdoft.assert_called_once_with(mock_sli_return['table_name'], 
                                               mock_py_tbl, 
                                               mock_sli_return['s3_client'], 
                                               mock_sli_return['ingestion_bucket'])

            # convert_to_parquet(dim_or_fact_table--return of mdoft)
            mock_ctp.assert_called_once_with(mock_mdoft_return, 
                                             mock_sli_return['table_name'])
            
            mock_mdoftk.assert_called_once_with(mock_sli_return['table_name'],
                                                mock_sli_return['timestamp_string'])

            # 2️⃣ Assert the exact calls in order
            mock_uts3.assert_called_once_with(
                            mock_sli_return['s3_client'], 
                            mock_sli_return['proc_bucket'],  
                            'mock_dim_or_fact_keystring', 
                            mock_pq),








# @pytest.mark.skip
def test_logs_first_info_message_correctly(general_setup, caplog):
    # Arrange:
    (mock_event, 
     mock_py_tbl, 
     mock_json_tbl, 
     mock_mdoft_return, 
     mock_pq, 
     mock_sli_return, 
     mock_ts,
     mock_sli_return_1 ) = general_setup
    
    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.INFO, logger="")
    
    with patch('src.second_lambda.second_lambda_handler.second_lambda_init') as mock_sli, \
         patch('src.second_lambda.second_lambda_handler.read_from_s3') as mock_rfs3, \
         patch('src.second_lambda.second_lambda_handler.make_dim_or_fact_table') as mock_mdoft, \
         patch('src.second_lambda.second_lambda_handler.convert_to_parquet') as mock_ctp, \
         patch('src.second_lambda.second_lambda_handler.upload_to_s3') as mock_uts3, \
         patch('src.second_lambda.second_lambda_handler.is_first_run_of_pipeline') as mock_ifrop, \
         patch('src.second_lambda.second_lambda_handler.create_dim_date_Parquet') as mock_cddP:     
        mock_sli.return_value = mock_sli_return
        mock_rfs3.return_value = mock_json_tbl
        mock_mdoft.return_value = mock_mdoft_return
        mock_ctp.return_value = mock_pq

        # Act:
        second_lambda_handler(mock_event, 'context')

        # print("MESSAGES:", caplog.messages)  # debug 
        assert any(info_lookup['info_0'] in msg for msg in caplog.messages)
        assert any(info_lookup['info_1'] in msg for msg in caplog.messages)



# @pytest.mark.skip
def test_handler_code_stops_if_table_is_department(general_setup):
    # Arrange:
    (mock_event, 
     mock_py_tbl, 
     mock_json_tbl, 
     mock_mdoft_return, 
     mock_pq, 
     mock_sli_return, 
     mock_ts,
     mock_sli_return_1 ) = general_setup


    with patch('src.second_lambda.second_lambda_handler.second_lambda_init') as mock_sli:
        mock_sli.return_value = mock_sli_return_1

        # act:
        response = second_lambda_handler(mock_event, 'context')

        # assert:
        assert response == {
            "status": "Second lambda handler code skipped",
            "reason": "Because table_name is 'department' "
                           }
                  

    