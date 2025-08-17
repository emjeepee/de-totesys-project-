import decimal
import datetime
from unittest.mock import Mock, patch
import pytest

from src.first_lambda.first_lambda_utils.read_table import read_table
from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db





@pytest.fixture
def test_list():
    # read_table(table_name: str, conn: Connection, after_time: str)
    table_name = "transactions"
    mock_conn = Mock()
    mock_conn.run.return_value = 'nothing'
    after_time = "2025-06-04T08:28:12"

    # mock the return value of 
    # get_column_names(conn, table_name):
    mock_gcn_return = [['transaction_id'], ['transaction_type'], ['sales_order_id'], ['purchase_order_id'], ['created_at'], ['last_updated']]

    mock_gcn_return_cleaned = ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated']

    # mock the return value of 
    # get_updated_rows(conn, after_time, table_name):
    mock_gur_return = [ 
    [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), decimal.Decimal(3.1415)],
    [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), decimal.Decimal(3.1416)],
    [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), decimal.Decimal(3.1417)]
                      ]   

    mock_cv_return =   [ 
    [20496, 'SALE', 14504, None, '2025-06-04T08:58:10.006000', 3.1415],
    [20497, 'SALE', 14505, None, '2025-06-04T09:26:09.972000', 3.1416],
    [20498, 'SALE', 14506, None, '2025-06-04T09:29:10.166000', 3.1417]
                      ]
    

    mock_mtd_return = [{}, {}, {}]

    mock_rt_return = { 'transactions': [
    {'transaction_id': 20496, 'transaction_type': 'SALE', 'sales_order_id': 14504, 'purchase_order_id': None, 'created_at': '2025-06-04T08:58:10.006000', 'last_updated': 3.1415},
    {'transaction_id': 20497, 'transaction_type': 'SALE', 'sales_order_id': 14505, 'purchase_order_id': None, 'created_at': '2025-06-04T09:26:09.972000', 'last_updated':3.1416},
    {'transaction_id': 20498, 'transaction_type': 'SALE', 'sales_order_id': 14506, 'purchase_order_id': None, 'created_at': '2025-06-04T09:29:10.166000', 'last_updated':3.1417}
                                 ]
               }


    mock_values = [
        table_name, 
        mock_conn, 
        after_time, 
        mock_gcn_return, 
        mock_gur_return, 
        mock_cv_return, 
        mock_mtd_return, 
        mock_gcn_return_cleaned, # ['transaction_id', 'transaction_type', etc]
        mock_rt_return
        ]

    return mock_values




# @pytest.mark.skip
def test_read_table_returns_dict(test_list):
    # Arrange:
    # read_table(table_name: str, conn: Connection, after_time: str)

    expected = dict
    
    with patch("src.first_lambda.first_lambda_utils.read_table.get_column_names", return_value = test_list[3]),  \
         patch("src.first_lambda.first_lambda_utils.read_table.get_updated_rows", return_value = test_list[4]):
         # Act
         response = read_table(test_list[0], test_list[1], test_list[2] )
         result = type(response)
    
    # Assert:
    assert result == expected








def test_calls_all_functions_with_correct_args(test_list):
    # Arrange:
    table_name = test_list[0]
    mock_conn = test_list[1]
    after_time = test_list[2]

    # Arrange, act and assert:
    # patch the utility functions that 
    # read_table calls:
    with patch("src.first_lambda.first_lambda_utils.read_table.get_column_names", return_value = test_list[3])  as mock_gcn,  \
         patch("src.first_lambda.first_lambda_utils.read_table.get_updated_rows", return_value = test_list[4])  as mock_gur,  \
         patch("src.first_lambda.first_lambda_utils.read_table.convert_values", return_value = test_list[5])  as mock_cv,  \
         patch("src.first_lambda.first_lambda_utils.read_table.make_row_dicts", return_value = test_list[6])  as mock_mrd:
         
         read_table(table_name, mock_conn, after_time)

        # Assert:
         mock_gcn.assert_called_once_with(mock_conn, table_name)
         mock_gur.assert_called_once_with(mock_conn, after_time, table_name) 
         # convert_values(query_result_1), returns cleaned_rows:          
         mock_cv.assert_called_once_with(test_list[4])
         # make_row_dicts(clean_col_names, cleaned_rows):
         mock_mrd.assert_called_once_with(test_list[7], test_list[5])



    


# @pytest.mark.skip
def test_read_table_returns_expected_dict(test_list):
    # Arrange
    expected = test_list[8]
    table_name = test_list[0]
    mock_conn = test_list[1]
    after_time = test_list[2]

    # Arrange, act and assert:
    # patch the utility functions that 
    # read_table calls:
    with patch("src.first_lambda.first_lambda_utils.read_table.get_column_names", return_value = test_list[3])  as mock_gcn,  \
         patch("src.first_lambda.first_lambda_utils.read_table.get_updated_rows", return_value = test_list[4])  as mock_gur,  \
         patch("src.first_lambda.first_lambda_utils.read_table.convert_values", return_value = test_list[5])  as mock_cv:  \
         # patch("src.first_lambda.first_lambda_utils.read_table.make_row_dicts", return_value = test_list[6])  as mock_mrd:
         
         result = read_table(table_name, mock_conn, after_time)
         assert result == expected    








# @pytest.mark.skip
def test_read_table_returns_expected_dict_with_real_connection_to_tote_sys_db(test_list):
    # Arrange:
    conn = conn_to_db('TOTE_SYS')
    expected_1 = dict
    expected_2 = list
    expected_3 = 'sales_order'
    expected_4 = True

    # , close_db

    # Act: 
    # read_table(table_name: str, conn: Connection, after_time: str)
    response = read_table('sales_order', conn, test_list[2] ) # a dict: {'sales_order': [{}, {}, {}, etc]}
    close_db(conn)

    result_key, result_value = next(iter(response.items()))
    result_1 = type(response)       # check the returned object is of type dict
    result_2 = type(result_value)   # check the returned object's key's value is of type list
    result_3 = result_key           # check the returned object's key is correct
    maxim = len(result_value)       # check that every object in the returned object's key's value is a dict
    result_4 = True
    for i in range(maxim):
        if type(result_value[i]) != dict:
            result_4 = False
            break 


    # Assert:
    result_1 = expected_1
    result_2 = expected_2
    result_3 = expected_3
    result_4 = expected_4






# @pytest.mark.skip
def test_read_table_raises_runtime_error(test_list):
    # Arrange: patch get_column_names so it raises
    with patch("src.first_lambda.first_lambda_utils.read_table.get_column_names",
               side_effect=RuntimeError("DB connection failed")):
        # Act + Assert
        with pytest.raises(RuntimeError) as e:
            read_table(test_list[0], None, test_list[2])

            # Extra check: exception message
            assert "DB connection failed" in str(e.value)
    