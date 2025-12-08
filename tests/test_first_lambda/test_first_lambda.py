import pytest
import boto3
import logging

from moto import mock_aws
from unittest.mock import Mock, patch

from src.first_lambda.first_lambda_handler import first_lambda_handler
from src.first_lambda.first_lambda_utils.info_lookup import info_lookup




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
        
        # values to put in mock 
        # dictionary that mock 
        # first_lambda_init() 
        # returns (plus 
        # S3_client and 
        # bucket_name_empty 
        # above):        
        mock_tables = ['design', 'sales_orders', 'transactions'] # list of names of tables of interest
        
        mock_conn = Mock() # mock pg8000.native.Connection object
        mock_cdb = Mock() # mock close_db

        # mock return value of 
        # get_env_vars():
        mock_gev_return_value = { 
            'ing_bucket_name': bucket_name_empty,
            'tables'     : mock_tables,                     
            's3_client'  : S3_client,  
            'conn'       : mock_conn,
            'close_db'   : mock_cdb,
            'proc_bucket_name': 'mock_proc_bucket_name'
                                 }    
        
        # mock the return value of 
        # change_aftertime_timestamp():
        mock_cats_return_value = "1900-01-01 00:00:00"

        # mock the return value of 
        # get_data_from_db():
        mock_gdfd_return_value = [{}, {}, {}]


        #mock the return value 
        # of reorder_list():
        mock_rl_return_value = [{}, {}, {}]

        # mock True return value of 
        # is_first_run_of_pipeline()
        mock_ifrop_True_return_value = True 

        # mock False return value of 
        # is_first_run_of_pipeline()
        mock_ifrop_False_return_value = False 

        # mock return value of 
        # make_updated_tables():
        mock_mut_return_value = ['mock_mut_return_value']




        
        yield S3_client, \
            bucket_name_empty, \
            bucket_name_with_objs, \
            mock_gev_return_value, \
            mock_cats_return_value, \
            mock_gdfd_return_value, \
            mock_rl_return_value, \
            mock_ifrop_True_return_value, \
            mock_ifrop_False_return_value, \
            mock_mut_return_value



def test_integrates_utility_functions_correctly_when_1st_ever_run_of_pipeline(general_setup):
    """
    This test employs the value of 
    mock_ifrop_True_return_value 
    (ie True) as the return value 
    of mock_ifrop.
    """

    # Arrange:
    (S3_client, 
     bucket_name_empty, 
     bucket_name_with_objs, 
     mock_gev_return_value,
     mock_cats_return_value,
     mock_gdfd_return_value,
     mock_rl_return_value,
     mock_ifrop_True_return_value, 
     mock_ifrop_False_return_value, 
     mock_mut_return_value
     ) = general_setup

    # patch all functions that first_lambda_handler
    # calls without having them passed in:
    with patch('src.first_lambda.first_lambda_handler.get_env_vars') as mock_gev, \
         patch('src.first_lambda.first_lambda_handler.change_after_time_timestamp') as mock_catts, \
         patch('src.first_lambda.first_lambda_handler.get_data_from_db') as mock_gdfd, \
         patch('src.first_lambda.first_lambda_handler.reorder_list') as mock_rl, \
         patch('src.first_lambda.first_lambda_handler.is_first_run_of_pipeline') as mock_ifrop, \
         patch('src.first_lambda.first_lambda_handler.write_tables_to_ing_buck') as mock_wttib, \
         patch('src.first_lambda.first_lambda_handler.make_updated_tables') as mock_mut, \
         patch('src.first_lambda.first_lambda_handler.read_table') as mock_rt :
        
        mock_gev.return_value  = mock_gev_return_value 
        mock_catts.return_value = mock_cats_return_value
        mock_gdfd.return_value = mock_gdfd_return_value
        mock_rl.return_value = mock_rl_return_value
        mock_ifrop.return_value = mock_ifrop_True_return_value
        mock_mut.return_value = mock_mut_return_value

        # Act:
        first_lambda_handler(None, None)

        # Assert:
        mock_gev.assert_called_once()

        mock_catts.assert_called_once_with(
            mock_gev_return_value['ing_bucket_name'], 
            mock_gev_return_value['s3_client'], 
            "***timestamp***", 
            "1900-01-01 00:00:00"
                                    )

        mock_gdfd.assert_called_once_with(
            mock_gev_return_value['tables'], 
            mock_cats_return_value,
            mock_gev_return_value['conn'],
            mock_rt
            )

        mock_rl.assert_called_once_with(
                mock_gdfd_return_value, 
                "address", 
                "department")


        mock_ifrop.assert_called_once_with(
            'mock_proc_bucket_name',
            mock_gev_return_value['s3_client']
                                        )
        
        mock_wttib.assert_called_once_with(
            mock_gev_return_value['s3_client'],
            mock_gev_return_value['ing_bucket_name'],
            mock_rl_return_value
                                          )
        
        mock_gev_return_value["close_db"].assert_called_once_with(mock_gev_return_value["conn"])



def test_integrates_utility_functions_correctly_when_2nd_plus_run_of_pipeline(general_setup):
    """
    This test employs the value of 
    mock_ifrop_False_return_value 
    (ie False) as the return value 
    of mock_ifrop.
    """
    # Arrange:
    (S3_client, 
     bucket_name_empty, 
     bucket_name_with_objs, 
     mock_gev_return_value,
     mock_cats_return_value,
     mock_gdfd_return_value,
     mock_rl_return_value,
     mock_ifrop_True_return_value, 
     mock_ifrop_False_return_value, 
     mock_mut_return_value
     ) = general_setup

    # patch all functions that first_lambda_handler
    # calls without having them passed in:
    with patch('src.first_lambda.first_lambda_handler.get_env_vars') as mock_gev, \
         patch('src.first_lambda.first_lambda_handler.change_after_time_timestamp') as mock_catts, \
         patch('src.first_lambda.first_lambda_handler.get_data_from_db') as mock_gdfd, \
         patch('src.first_lambda.first_lambda_handler.reorder_list') as mock_rl, \
         patch('src.first_lambda.first_lambda_handler.is_first_run_of_pipeline') as mock_ifrop, \
         patch('src.first_lambda.first_lambda_handler.write_tables_to_ing_buck') as mock_wttib, \
         patch('src.first_lambda.first_lambda_handler.make_updated_tables') as mock_mut, \
         patch('src.first_lambda.first_lambda_handler.read_table') as mock_rt :
        
        mock_gev.return_value  = mock_gev_return_value 
        mock_catts.return_value = mock_cats_return_value
        mock_gdfd.return_value = mock_gdfd_return_value
        mock_rl.return_value = mock_rl_return_value
        mock_ifrop.return_value = mock_ifrop_False_return_value
        mock_mut.return_value = mock_mut_return_value

        # Act:
        first_lambda_handler(None, None)

        # Assert:
        mock_gev.assert_called_once()

        mock_catts.assert_called_once_with(
            mock_gev_return_value['ing_bucket_name'], 
            mock_gev_return_value['s3_client'], 
            "***timestamp***", 
            "1900-01-01 00:00:00"
                                    )

        mock_gdfd.assert_called_once_with(
            mock_gev_return_value['tables'], 
            mock_cats_return_value,
            mock_gev_return_value['conn'],
            mock_rt
            )

        mock_rl.assert_called_once_with(
                mock_gdfd_return_value, 
                "address", 
                "department")


        mock_ifrop.assert_called_once_with(
            'mock_proc_bucket_name',
            mock_gev_return_value['s3_client']
                                        )
        
        
        mock_mut.assert_called_once_with(mock_rl_return_value,
                                         mock_gev_return_value['s3_client'],
                                         mock_gev_return_value['ing_bucket_name'],
                                         )


        mock_wttib.assert_called_once_with(
            mock_gev_return_value['s3_client'],
            mock_gev_return_value['ing_bucket_name'],
            mock_mut_return_value
                                          )
        
        mock_gev_return_value["close_db"].assert_called_once_with(mock_gev_return_value["conn"])




# @pytest.mark.skip
def test_logs_info_messages_correctly(general_setup, caplog):
    # Arrange:
    (S3_client, 
     bucket_name_empty, 
     bucket_name_with_objs, 
     mock_gev_return_value,
     mock_cats_return_value,
     mock_gdfd_return_value,
     mock_rl_return_value,
     mock_ifrop_True_return_value, 
     mock_ifrop_False_return_value, 
     mock_mut_return_value
     ) = general_setup
    
    caplog.set_level(logging.INFO)

    # patch all functions that first_lambda_handler
    # calls without having them passed in:
    with patch('src.first_lambda.first_lambda_handler.get_env_vars') as mock_gev, \
         patch('src.first_lambda.first_lambda_handler.change_after_time_timestamp') as mock_catts, \
         patch('src.first_lambda.first_lambda_handler.get_data_from_db') as mock_gdfd, \
         patch('src.first_lambda.first_lambda_handler.reorder_list') as mock_rl, \
         patch('src.first_lambda.first_lambda_handler.is_first_run_of_pipeline') as mock_ifrop, \
         patch('src.first_lambda.first_lambda_handler.write_tables_to_ing_buck') as mock_wttib, \
         patch('src.first_lambda.first_lambda_handler.make_updated_tables') as mock_mut, \
         patch('src.first_lambda.first_lambda_handler.read_table') as mock_rt :
        
        mock_gev.return_value  = mock_gev_return_value 
        mock_catts.return_value = mock_cats_return_value
        mock_gdfd.return_value = mock_gdfd_return_value
        mock_rl.return_value = mock_rl_return_value

        # Act:
        first_lambda_handler(None, None)
        
        # # capture INFO-level logs from root logger
    
        # Assert — check that each message appears
        assert any(info_lookup['info_0'] in msg for msg in caplog.messages)        
        assert any(info_lookup['info_1'] in msg for msg in caplog.messages)        
        assert any(info_lookup['info_2'] in msg for msg in caplog.messages)        
        assert any(info_lookup['info_3'] in msg for msg in caplog.messages)        
