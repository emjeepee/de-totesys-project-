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

    with patch('src.third_lambda.third_lambda_utils.make_insert_queries_from_parquet.duckdb.connect') as mock_connect, \
        patch('src.third_lambda.third_lambda_utils.make_insert_queries_from_parquet.read_parquet_from_buffer') as mock_rpfb, \
        patch('src.third_lambda.third_lambda_utils.make_insert_queries_from_parquet.make_list_of_query_strings') as mock_mloqs:

        mc_return = Mock()
        mc_return.close.return_value = None
        mock_connect.return_value = mc_return
        mock_mloqs.return_value =['mock_list_of_query_strings']
        mock_rpfb.return_value =['obj_at_ind_0', 'obj_at_ind_1']

        # act:
        response = make_insert_queries_from_parquet(mock_pq_in_buff, mock_table_name)
        result = type(response)

        # assert:
        assert result == expected
        mock_connect.assert_called_once_with(':memory:')
        mock_rpfb.assert_called_once_with(mock_pq_in_buff, mc_return)
        mock_mloqs.assert_called_once_with('obj_at_ind_1', mock_table_name, 'obj_at_ind_0')


