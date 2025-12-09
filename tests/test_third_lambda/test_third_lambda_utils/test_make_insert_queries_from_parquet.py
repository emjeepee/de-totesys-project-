import pytest

from unittest.mock import Mock, patch, ANY
from decimal import Decimal

from src.third_lambda.third_lambda_utils.make_insert_queries_from_parquet import make_insert_queries_from_parquet


@pytest.fixture(scope="function")
def setup():
    mock_so_table = [
             {
              'sales_order_id': 15647, 
              'created_at': 'xxx', 
              'unit_price': Decimal('2.40')
              },

             {
              'sales_order_id': 15648, 
              'created_at': 'yyy', 
              'unit_price': Decimal('4.63')
              },
                        ]
    
    mock_table_name = 'sales_order',

    mock_pq_in_buff = Mock()



    
    yield mock_so_table, mock_table_name, mock_pq_in_buff





def test_calls_all_functions_correctly_and_returns_a_list(setup):
    # arrange:
    (mock_so_table, 
     mock_table_name, 
     mock_pq_in_buff, 
    ) = setup

    expected = list

    path_to_connect = ('src.third_lambda.'
                       'third_lambda_utils.'
                       'make_insert_queries_from_parquet.'
                       'duckdb.connect')
    
    path_to_read_pq = ('src.third_lambda.'
                        'third_lambda_utils.'
                        'make_insert_queries_from_parquet.'
                        'read_parquet_from_buffer')

    path_tp_mloqs = ('src.third_lambda.'
                    'third_lambda_utils.'
                    'make_insert_queries_from_parquet.'
                    'make_list_of_query_strings')
    
    path_tp_gcar = ('src.third_lambda.'
                    'third_lambda_utils.'
                    'make_insert_queries_from_parquet.'
                    'get_columns_and_rows')

    with patch(path_to_connect) as mock_connect, \
        patch(path_to_read_pq) as mock_rpfb, \
        patch(path_tp_gcar) as mock_gcar, \
        patch(path_tp_mloqs) as mock_mloqs:

        mock_conn = Mock()
        mock_conn.close = Mock()
        mock_connect.return_value = mock_conn
        mock_mloqs.return_value =['mock_list_of_query_strings']
        mock_DDB_q_result_obj = Mock()
        mock_rpfb.return_value = mock_DDB_q_result_obj

        mock_row_cols = [
            'col_name_1, col_name_2, col_name_3',
            [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
                        ]
        
        mock_gcar.return_value = mock_row_cols



        # act:
        response = make_insert_queries_from_parquet(mock_pq_in_buff, mock_table_name)
        result = type(response)

        # assert:
        assert result == expected
        mock_connect.assert_called_once_with(':memory:')
        mock_rpfb.assert_called_once_with(mock_pq_in_buff, mock_conn)
        mock_gcar.assert_called_once_with(mock_DDB_q_result_obj)
        mock_mloqs.assert_called_once_with(mock_row_cols[1], mock_table_name, mock_row_cols[0])
        mock_conn.close.assert_called_once()

