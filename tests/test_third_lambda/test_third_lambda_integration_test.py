
import pytest

from unittest.mock import Mock, patch, ANY

from src.third_lambda.third_lambda_handler import third_lambda_handler




def test_third_lambda_function_correctly_integrates_all_utility_functions_it_calls():

    # Arrange, act and assert:
    with patch('src.third_lambda.third_lambda_handler.third_lambda_init') as mock_tla, \
         patch('src.third_lambda.third_lambda_handler.make_pandas_dataframe') as mock_mpd, \
         patch('src.third_lambda.third_lambda_handler.make_SQL_queries') as mock_msq, \
         patch('src.third_lambda.third_lambda_handler.make_SQL_queries_to_warehouse') as mock_msqtw, \
         patch('src.third_lambda.third_lambda_handler.close_db') as mock_cdb: \

        mock_tla.return_value = {'proc_bucket': '11-processed_bucket',
                                 's3_client': 'mock_s3_client',
                                 'object_key' : 'mock_object_key',
                                 'table_name': 'mock_table_name',
                                 'conn': 'mock_conn',
                                 'close_db': 'mock_close_db'
                                 }
        mock_df = Mock()
        mock_ql = Mock()
        mock_mpd.return_value = mock_df
        mock_msq.return_value = mock_ql

        # third_lambda_init(event, conn_to_db, close_db, boto3.client('s3'))
        third_lambda_handler(ANY, ANY)

        # mock_tla.assert_called_once_with()
        mock_tla.assert_called_once_with(ANY, ANY, ANY, ANY)
        
        # mock_mpd.assert_called_once_with('fail_arg', 'fail_arg', 'fail_arg')
        mock_mpd.assert_called_once_with('11-processed_bucket', 'mock_s3_client', 'mock_object_key')

        # mock_msq.assert_called_once_with('fail_arg', 'fail_arg')
        mock_msq.assert_called_once_with(mock_df, 'mock_table_name')

        # mock_msqtw.assert_called_once_with('fail_arg', 'fail_arg')
        mock_msqtw.assert_called_once_with(mock_ql, 'mock_conn')

        # close_conn(conn)
        # mock_cdb.assert_called_once_with('fail_arg')
        mock_cdb.assert_called_once_with('mock_conn')

