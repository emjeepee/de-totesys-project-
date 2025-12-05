import decimal
import datetime
import pytest
import os

from unittest.mock import Mock, patch, ANY


from src.first_lambda.first_lambda_utils.read_table import read_table
from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db


@pytest.fixture
def ts_ok():
    """
    Set value of IS_OLTP_OK
    to "True" to allow test of
    read_table() as if the 
    totesys database is still
    functioning.
    """
    
    os.environ["IS_OLTP_OK"] = "True"

    is_ts_ok = os.environ["IS_OLTP_OK"]
    is_ts_ok =  True if is_ts_ok.lower() == "true" else False 

    yield is_ts_ok

    # clean up:
    os.environ.pop("IS_OLTP_OK", None)
    os.environ["IS_OLTP_OK"] = "False"





@pytest.fixture
def ts_bad():
    """
    Set value of IS_OLTP_OK
    to "False" to allow test of
    read_table() as if the 
    totesys database is not
    functioning.
    """
    
    os.environ["IS_OLTP_OK"] = "False"

    is_ts_ok = os.environ["IS_OLTP_OK"]
    is_ts_ok =  True if is_ts_ok.lower() == "true" else False 

    yield is_ts_ok

    # no need for clean up
    # as database totesys 
    # is no longer working:










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
def test_read_table_returns_dict_if_totesys_OK(test_list, ts_ok):
    # Arrange:
    is_ts_ok = ts_ok

    if is_ts_ok:
        # read_table(table_name: str, conn: Connection, after_time: str)

        expected = dict
        
        with patch("src.first_lambda.first_lambda_utils.read_table.get_column_names", return_value = test_list[3]),  \
            patch("src.first_lambda.first_lambda_utils.read_table.get_updated_rows", return_value = test_list[4]):
            # Act
            response = read_table(test_list[0], test_list[1], test_list[2] )
            result = type(response)
        
        # Assert:
        assert result == expected








def test_calls_all_functions_with_correct_args_if_totesys_OK(test_list, ts_ok):
    # Arrange:
    is_ts_ok = ts_ok

    if is_ts_ok:

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
def test_read_table_returns_expected_dict(test_list, ts_ok):
    # Arrange:
    is_ts_ok = ts_ok

    if is_ts_ok:
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
def test_read_table_returns_expected_dict_with_real_connection_to_tote_sys_db_if_totesys_OK(test_list, ts_ok):
    # Arrange:
    is_ts_ok = ts_ok

    if is_ts_ok:
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


def test_returns_correct_dictionary_when_totesys_not_OK(test_list, ts_bad):
    # arrange:
    is_ts_ok = ts_bad
    # is_ts_ok = False
    expected_type = dict
    expected = {
                "design": [
                    {"col_1": "val_1", "col_2": "val_2"}, 
                    {"col_1": "val_1", "col_2": "val_2"}
                          ]
               }


    if is_ts_ok:
        with patch('src.first_lambda.first_lambda_utils.read_table.make_fake_so_table') as mfsot, \
             patch('src.first_lambda.first_lambda_utils.read_table.make_fake_de_table') as mfdet, \
             patch('src.first_lambda.first_lambda_utils.read_table.make_fake_ad_table') as mfadt, \
             patch('src.first_lambda.first_lambda_utils.read_table.make_fake_st_table') as mfstt, \
             patch('src.first_lambda.first_lambda_utils.read_table.make_fake_cu_table') as mfcut, \
             patch('src.first_lambda.first_lambda_utils.read_table.make_fake_cp_table') as mfcpt, \
             patch('src.first_lambda.first_lambda_utils.read_table.make_fake_dp_table') as mfdpt: \

            mfsot.return_value = [{}, {}]
            mfdet.return_value = [{"col_1": "val_1", "col_2": "val_2"}, {"col_1": "val_1", "col_2": "val_2"}]
            mfadt.return_value = [{}, {}]
            mfstt.return_value = [{}, {}]
            mfcut.return_value = [{"col_1": "val_1", "col_2": "val_2"}, {"col_1": "val_1", "col_2": "val_2"}]
            mfcpt.return_value = [{}, {}]
            mfdpt.return_value = [{}, {}]


            # act:
            response = read_table('design', ANY, ANY)
            result_type = type(response)
            result = response

            assert result_type == expected_type
            assert result == expected

    