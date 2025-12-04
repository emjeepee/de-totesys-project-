import pytest

from decimal import Decimal

from src.third_lambda.third_lambda_utils.make_insert_queries_from_parquet import make_insert_queries_from_parquet
from src.second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquet


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
    
    mock_table_name = 'sales_order'
    
    yield mock_so_table, mock_table_name





def test_returns_a_list(setup):
    # arrange:
    mock_so_table, mock_table_name = setup

    expected = list

    # convert_to_parquet() 
    # returns a BytesIO buffer 
    # that contains a table in 
    # Parquet format:
    pq_in_buff = convert_to_parquet(mock_so_table, mock_table_name)

    # act:
    response = make_insert_queries_from_parquet(pq_in_buff, mock_table_name)
    result = type(response)

    # assert:
    assert result == expected

    
    