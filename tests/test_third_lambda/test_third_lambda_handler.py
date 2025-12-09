import pytest
import logging

from unittest.mock import Mock, patch, ANY
from botocore.exceptions import ClientError
from io import BytesIO

from src.third_lambda.third_lambda_handler import third_lambda_handler
from src.third_lambda.third_lambda_utils.errors_lookup import errors_lookup
from src.third_lambda.third_lambda_utils.info_lookup import info_lookup




@pytest.fixture(scope="function")
def setup():
    path_to_tli = ('src.third_lambda.'
                    'third_lambda_handler.'
                    'third_lambda_init')
    

    path_to_gip = ('src.third_lambda.'
                   'third_lambda_handler.'
                   'get_inbuffer_parquet')


    path_to_miqfp = ('src.third_lambda.'
                    'third_lambda_handler.'
                    'make_insert_queries_from_parquet')

    path_to_msqtw = ('src.third_lambda.'
                    'third_lambda_handler.'
                    'make_SQL_queries_to_warehouse')
    
    path_to_close_db = ('src.third_lambda.'
                        'third_lambda_handler.'
                        'close_db')
    
    path_to_bio = ('src.third_lambda.'
                    'third_lambda_handler.'
                    'io.BytesIO')

    path_to_client = ('src.third_lambda.'
                      'third_lambda_handler.'
                      'boto3.client')


    yield (path_to_tli, 
           path_to_gip, 
           path_to_miqfp, 
           path_to_msqtw, 
           path_to_close_db, 
           path_to_bio,
           path_to_client)


def test_correctly_integrates_all_utility_functions_and_logs_info_correctly_(set_up, caplog):
    (path_to_tli, 
    path_to_gip, 
    path_to_miqfp, 
    path_to_msqtw, 
    path_to_close_db,
    path_to_bio,
    path_to_client) = setup

    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.INFO, logger="") # logger="" means any logger

    # Arrange, act and assert:


    with patch(path_to_tli) as mock_tli, \
         patch(path_to_gip) as mock_gip, \
         patch(path_to_miqfp) as mock_miqfp, \
         patch(path_to_msqtw) as mock_msqtw, \
         patch(path_to_close_db) as mock_cdb:
            

        with patch(path_to_bio) as mock_iobio, \
             patch(path_to_client) as mock_bt3cl :
            mock_bt3cl.return_value = 'mock_s3_client'
            mock_iobio.return_value = 'mock_pq_buff'
            mock_miqfp.return_value = 'queries'
            mock_s3_client = Mock() # in real life boto3.client('s3')
            mock_bytes = b"fake-bytes"
            mock_sbo = Mock()# mock streaming body object
            mock_sbo.read.return_value = mock_bytes
            mock_s3_client.get_object.return_value = {"Body": mock_sbo}
           
            mock_tli.return_value = {'proc_bucket': '11-processed_bucket',
                                    's3_client': mock_s3_client,
                                    'object_key' : 'mock_object_key',
                                    'table_name': 'mock_table_name',
                                    'conn': 'mock_conn',
                                    'close_db': 'mock_close_db'
                                    }
            mock_miqfp.return_value = 'mock_miqfp_return_value'

            # third_lambda_handler(event, context)
            third_lambda_handler(ANY, ANY)

            # third_lambda_init(event, conn_to_db, close_db, boto3.client('s3'))    
            mock_tli.assert_called_once_with(ANY, mock_ctdb, mock_cdb, 'mock_s3_client')
            
            mock_miqfp.assert_called_once_with('mock_pq_buff', mock_tli.return_value['table_name'])
            
            mock_msqtw.assert_called_once_with('mock_miqfp_return_value', 'mock_conn')

            mock_cdb.assert_called_once_with('mock_conn')


        info_msg_1 = info_lookup["info_1"] + f"{mock_tli.return_value['table_name']}"
        assert any(info_lookup['info_0'] in msg for msg in caplog.messages)
        assert any(info_msg_1 in msg for msg in caplog.messages)



def test_raises_ClientError_when_reading_processed_bucket_fails_and_logs_exception_correctly(caplog):
    # Arrange, act and assert:

    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="") # logger="" means any logger

    with patch('src.third_lambda.third_lambda_handler.third_lambda_init') as mock_tli, \
         patch('src.third_lambda.third_lambda_handler.make_insert_queries_from_parquet') as mock_miqfp, \
         patch('src.third_lambda.third_lambda_handler.make_SQL_queries_to_warehouse') as mock_msqtw, \
         patch('src.third_lambda.third_lambda_handler.close_db') as mock_cdb:
            
            

            mock_miqfp.return_value = 'queries'

            mock_s3_client = Mock() # in real life boto3.client('s3')
            # for ClientError():
            error_response = {
                   "Error": {
                    "Code": "404",
                    "Message": "Not Found"
                            }
                             }
            mock_s3_client.get_object.side_effect = ClientError(error_response, "GetObject")
            
            mock_tli.return_value = {'proc_bucket': '11-processed_bucket',
                                    's3_client': mock_s3_client,
                                    'object_key' : 'mock_object_key',
                                    'table_name': 'mock_table_name',
                                    'conn': 'mock_conn',
                                    'close_db': 'mock_close_db'
                                    }
            mock_miqfp.return_value = 'mock_miqfp_return_value'
    
            with pytest.raises(ClientError):
                # third_lambda_init(event, conn_to_db, close_db, boto3.client('s3'))
                third_lambda_handler(ANY, ANY)
 
    # check that error 
    # was logged:
    err_msg = errors_lookup['err_0'] + f'{mock_tli.return_value['table_name']}'
    assert any(err_msg in msg for msg in caplog.messages)

 

