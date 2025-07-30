import decimal
import datetime
from unittest.mock import Mock
import pytest

from src.first_lambda.first_lambda_utils.read_table import read_table




def test_read_table_returns_expected_dict():
    # Arrange

    table_name = "transactions"
    after_time = "2025-06-04T08:28:12"

    mock_conn_1st_return = [ 
    [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), decimal.Decimal(3.1415)],
    [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), decimal.Decimal(3.1416)],
    [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), decimal.Decimal(3.1417)]
                            ]

    mock_conn_2nd_return = [['transaction_id'], ['transaction_type'], ['sales_order_id'], ['purchase_order_id'], ['created_at'], ['last_updated']]

    expected = { 'transactions': [
    {'transaction_id': 20496, 'transaction_type': 'SALE', 'sales_order_id': 14504, 'purchase_order_id': None, 'created_at': '2025-06-04T08:58:10.006000', 'last_updated': 3.1415},
    {'transaction_id': 20497, 'transaction_type': 'SALE', 'sales_order_id': 14505, 'purchase_order_id': None, 'created_at': '2025-06-04T09:26:09.972000', 'last_updated':3.1416},
    {'transaction_id': 20498, 'transaction_type': 'SALE', 'sales_order_id': 14506, 'purchase_order_id': None, 'created_at': '2025-06-04T09:29:10.166000', 'last_updated':3.1417}
                                 ]
               }

    # Create a mock object to mock 
    # conn:
    mock_conn = Mock()  
    
    # Successive members of the list 
    # below are what mock_conn
    # returns on successive calls:
    mock_conn.run.side_effect = [   
        mock_conn_1st_return, # returned after 1st call
        mock_conn_2nd_return # returned after 2nd call
                                ]


    # Act
    result = read_table(table_name, mock_conn, after_time )
    

    # Assert
    assert result == expected
    assert mock_conn.run.call_count == 2


