import pytest
import logging

from datetime import datetime
from unittest.mock import Mock, ANY
from pg8000.native import Error

from src.first_lambda.first_lambda_utils.get_updated_rows import get_updated_rows
from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup




@pytest.fixture
def test_list():
    mock_returned = [ [20496, 'SALE', 14504, None, datetime(2025, 6, 4, 8, 58, 10, 6000), datetime(2025, 6, 4, 8, 58, 10, 6000)],
    [20497, 'SALE', 14505, None, datetime(2025, 6, 4, 9, 26, 9, 972000), datetime(2025, 6, 4, 9, 26, 9, 972000)],
    [20498, 'SALE', 14506, None, datetime(2025, 6, 4, 9, 29, 10, 166000), datetime(2025, 6, 4, 9, 29, 10, 166000)]]

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
    response = get_updated_rows(mock_conn, "2025-06-04T08:28:12", 'design')
    result   = type(response)

    # Assert
    assert result == expected






# @pytest.mark.skip
def test_returns_list_of_rows(test_list):
    # Arrange
    mock_returned = test_list

    mock_conn = Mock()
    mock_conn.run.return_value = mock_returned

    # Act
    response = get_updated_rows(mock_conn, ANY, ANY)
    result_1 = response[0]
    result_2 = response[1]
    result_3 = response[2]

    expected_1 = [20496, 'SALE', 14504, None, datetime(2025, 6, 4, 8, 58, 10, 6000), datetime(2025, 6, 4, 8, 58, 10, 6000)]
    expected_2 = [20497, 'SALE', 14505, None, datetime(2025, 6, 4, 9, 26, 9, 972000), datetime(2025, 6, 4, 9, 26, 9, 972000)]
    expected_3 = [20498, 'SALE', 14506, None, datetime(2025, 6, 4, 9, 29, 10, 166000), datetime(2025, 6, 4, 9, 29, 10, 166000)]

    # Assert:
    assert expected_1 == result_1
    assert expected_2 == result_2
    assert expected_3 == result_3






# @pytest.mark.skip
def test_returns_only_updated_rows_when_real_connection_made_to_tote_sys_database():
    # Arrange
    conn = conn_to_db('TOTE_SYS')  
    timestamp_fail = "2026-06-04T08:28:12"
    timestamp = "2025-06-04T08:28:12"
    # Convert timestamp into datetime object:
    timestamp_fail_dt = datetime.fromisoformat(timestamp_fail)
    timestamp_dt = datetime.fromisoformat(timestamp)
    
    # design table's columns are:
    # 'design_id', 'created_at', 'design_name', 'file_location', 'file_name', 'last_updated', 

    # Test that the datetime object at index[5]
    # of every member list in the list of lists 
    # returned by get_updated_rows() represents 
    # a time creater than the time represented 
    # by timestamp:

    # Act and assert:
    response = get_updated_rows(conn, timestamp, 'design')
    max = len(response)
    for i in range(max):
        # make test fail:
        # assert response[i][5] > timestamp_fail_dt
        assert response[i][5] > timestamp_dt

    close_db(conn)




def test_that_the_function_logs_correctly(caplog):
    # arrange: 
    timestamp = "2025-06-04T08:28:12"
    # logging.ERROR below deals 
    # with logger.exception() too:
    caplog.set_level(logging.ERROR, logger="get_column_names")

    # Create a mock connection object:
    mock_conn = Mock()
    # Make the .run() method raise the exception
    mock_conn.run.side_effect = Error("get_updated_rows() raised Error, an OLTP DB error")

    # patch conn_obj.run(query) 
    # and get it to return an 
    # Error exception:
    # Act
    # ensure test can fail:
    # result = get_column_names(conn, 'custard')
    with pytest.raises(Error):
        # ensure test can fail:
        # result = get_updated_rows('mock_conn', 'timestamp', 'table_name_doesnt_matter')
        get_updated_rows(mock_conn, timestamp, 'table_name')

    # ensure test can fail:
    # expected_err_msg = 'aaa'
    expected_err_msg = errors_lookup['err_3'] + 'table_name'
    assert any(expected_err_msg in msg for msg in caplog.messages)
        




