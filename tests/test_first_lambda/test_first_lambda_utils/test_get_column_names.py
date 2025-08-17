import pytest
from unittest.mock import Mock, ANY
from src.first_lambda.first_lambda_utils.get_column_names import get_column_names
from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db





@pytest.fixture
def test_list():

    mock_returned =     [
       ['design_id'],
       ['created_at'],
       ['design_name'],
       ['file_location'],
       ['file_name'],
       ['last_updated'],
                     ]

    return mock_returned




# @pytest.mark.skip
def test_returns_a_list(test_list):
    # Arrange
    mock_returned = test_list
    mock_conn = Mock()
    mock_conn.run.return_value = mock_returned
    expected = type([])

    # Act
    # def contact_tote_sys_db(conn_obj, opt: int, after_time: str, table_name: str):
    response = get_column_names(mock_conn, ANY)
    result   = type(response)

    # Assert
    assert result == expected
    





# @pytest.mark.skip
# test_lists is a tiple:
def test_returns_list_of_column_names(test_list):
    # Arrange
    mock_returned = test_list
    mock_conn = Mock()
    mock_conn.run.return_value = mock_returned

    expected_1 = ['design_id']
    expected_2 = ['created_at']
    expected_3 = ['design_name']
    expected_4 = ['file_location']
    expected_5 = ['file_name']
    expected_6 = ['last_updated']

    # Act
    # (conn_obj, opt: int, after_time: str, table_name: str)
    response = get_column_names(mock_conn, ANY)
    result_1 = response[0]
    result_2 = response[1]
    result_3 = response[2]
    result_4 = response[3]
    result_5 = response[4]
    result_6 = response[5]

    # Assert
    assert expected_1 == result_1
    assert expected_2 == result_2
    assert expected_3 == result_3
    assert expected_4 == result_4
    assert expected_5 == result_5
    assert expected_6 == result_6




# @pytest.mark.skip
def test_returns_a_list_of_column_names_after_real_connection_to_tote_sys_database():
    # Arrange
    conn = conn_to_db('TOTE_SYS')
    expected = [
                ['counterparty_id'], 
                ['counterparty_legal_name'], 
                ['legal_address_id'], 
                ['commercial_contact'], 
                ['delivery_contact'], 
                ['created_at'], 
                ['last_updated']
               ] 
    

    # Act
    result = get_column_names(conn, 'counterparty')
    close_db(conn)

    # Assert
    assert result == expected
    


