import pytest

from unittest.mock import Mock, patch, ANY

from src.third_lambda.third_lambda_handler import third_lambda_handler




def test_third_lambda_function_correctly_integrates_all_utility_functions_it_calls():

    # Arrange, act and assert:
    with patch('src.third_lambda.third_lambda_handler.third_lambda_init') as mock_tli, \
         patch('src.third_lambda.third_lambda_handler.make_insert_queries_from_parquet') as mock_miqfp, \
         patch('src.third_lambda.third_lambda_handler.make_SQL_queries_to_warehouse') as mock_msqtw, \
         patch('src.third_lambda.third_lambda_handler.close_db') as mock_cdb:
            
            

            mock_miqfp.return_value = 'queries'

            mock_s3_client = Mock() # in real life boto3.client('s3')
            mock_s3_client.get_object.return_value = 'mock_pq_buff'
            
            mock_tli.return_value = {'proc_bucket': '11-processed_bucket',
                                    's3_client': mock_s3_client,
                                    'object_key' : 'mock_object_key',
                                    'table_name': 'mock_table_name',
                                    'conn': 'mock_conn',
                                    'close_db': 'mock_close_db'
                                    }
            mock_miqfp.return_value = 'mock_miqfp_return_value'

            # third_lambda_init(event, conn_to_db, close_db, boto3.client('s3'))
            third_lambda_handler(ANY, ANY)

            # mock_tla.assert_called_once_with()
            mock_tli.assert_called_once_with(ANY, ANY, ANY, ANY)
            
            # mock_mpd.assert_called_once_with('fail_arg', 'fail_arg', 'fail_arg')
            mock_miqfp.assert_called_once_with(ANY, mock_tli.return_value['table_name'])

            # mock_msqtw.assert_called_once_with('fail_arg', 'fail_arg')
            mock_msqtw.assert_called_once_with('mock_miqfp_return_value', 'mock_conn')

            # close_conn(conn)
            # mock_cdb.assert_called_once_with('fail_arg')
            mock_cdb.assert_called_once_with('mock_conn')



def test_raises_exception_when_reading_processed_bucket_fails():
        # Arrange, act and assert:
    with patch('src.third_lambda.third_lambda_handler.third_lambda_init') as mock_tli, \
         patch('src.third_lambda.third_lambda_handler.make_insert_queries_from_parquet') as mock_miqfp, \
         patch('src.third_lambda.third_lambda_handler.make_SQL_queries_to_warehouse') as mock_msqtw, \
         patch('src.third_lambda.third_lambda_handler.close_db') as mock_cdb:
            
            

            mock_miqfp.return_value = 'queries'

            mock_s3_client = Mock() # in real life boto3.client('s3')
            mock_s3_client.get_object.side_effect = RuntimeError()
            
            mock_tli.return_value = {'proc_bucket': '11-processed_bucket',
                                    's3_client': mock_s3_client,
                                    'object_key' : 'mock_object_key',
                                    'table_name': 'mock_table_name',
                                    'conn': 'mock_conn',
                                    'close_db': 'mock_close_db'
                                    }
            mock_miqfp.return_value = 'mock_miqfp_return_value'
    
            with pytest.raises(RuntimeError):
                # third_lambda_init(event, conn_to_db, close_db, boto3.client('s3'))
                third_lambda_handler(ANY, ANY)
 

def test_raises_exception_when_msqtw_fails():
    """
    msqtw is function make_insert_queries_from_parquet()
    """
    
    # Arrange, act and assert:
    with patch('src.third_lambda.third_lambda_handler.third_lambda_init') as mock_tli, \
         patch('src.third_lambda.third_lambda_handler.make_insert_queries_from_parquet') as mock_miqfp, \
         patch('src.third_lambda.third_lambda_handler.make_SQL_queries_to_warehouse') as mock_msqtw:
         # patch('src.third_lambda.third_lambda_handler.close_db') as mock_cdb:

            mock_miqfp.return_value = 'queries'

            mock_s3_client = Mock() # in real life boto3.client('s3')
            mock_s3_client.get_object.return_value = 'pq_buff'
            
            mock_tli.return_value = {'proc_bucket': '11-processed_bucket',
                                    's3_client': mock_s3_client,
                                    'object_key' : 'mock_object_key',
                                    'table_name': 'mock_table_name',
                                    'conn': 'mock_conn',
                                    'close_db': 'mock_close_db'
                                    }
            mock_miqfp.return_value = 'mock_miqfp_return_value'

            mock_msqtw.side_effect = RuntimeError()
    
            with pytest.raises(RuntimeError):
                # third_lambda_init(event, conn_to_db, close_db, boto3.client('s3'))
                third_lambda_handler(ANY, ANY)
 

