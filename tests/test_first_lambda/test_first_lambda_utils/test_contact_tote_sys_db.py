import pytest
from datetime import datetime
from pg8000 import ProgrammingError
from unittest.mock import Mock, patch, ANY
from src.first_lambda.first_lambda_utils.contact_tote_sys_db import contact_tote_sys_db











@pytest.fixture
def test_lists():
    mock_returned_1 = [ [20496, 'SALE', 14504, None, datetime(2025, 6, 4, 8, 58, 10, 6000), datetime(2025, 6, 4, 8, 58, 10, 6000)],
    [20497, 'SALE', 14505, None, datetime(2025, 6, 4, 9, 26, 9, 972000), datetime(2025, 6, 4, 9, 26, 9, 972000)],
    [20498, 'SALE', 14506, None, datetime(2025, 6, 4, 9, 29, 10, 166000), datetime(2025, 6, 4, 9, 29, 10, 166000)]]

    mock_returned_2 =     [
       ['design_id'],
       ['created_at'],
       ['design_name'],
       ['file_location'],
       ['file_name'],
       ['last_updated'],
                     ]

    # return a tuple containing
    # mock_returned_1 and mock_returned_2:
    return mock_returned_1, mock_returned_2




# @pytest.mark.skip
def test_contact_tote_sys_db_returns_a_list(test_lists):
    # Arrange
    mock_returned_list_2 = test_lists[1]
    mock_conn = Mock()
    mock_conn.run.return_value = mock_returned_list_2
    expected = type([])

    # Act
    # def contact_tote_sys_db(conn_obj, opt: int, after_time: str, table_name: str):
    response = contact_tote_sys_db(mock_conn, 2, ANY, ANY)
    result = type(response)

    # Assert
    assert result == expected
    





# @pytest.mark.skip
# test_lists is a tiple:
def test_contact_tote_sys_db_returns_list_of_column_names(test_lists):
    # Arrange
    mock_returned_list_2 = test_lists[1]
    # mock_returned_list_2 looks like this:
    #     [
    #    ['design_id'],
    #    ['created_at'],
    #    ['design_name'],
    #    ['file_location'],
    #    ['file_name'],
    #    ['last_updated'],
    #                  ]

    mock_conn = Mock()
    mock_conn.run.return_value = mock_returned_list_2

    expected_1 = ['design_id']
    expected_2 = ['created_at']
    expected_3 = ['design_name']
    expected_4 = ['file_location']
    expected_5 = ['file_name']
    expected_6 = ['last_updated']

    # Act
    # (conn_obj, opt: int, after_time: str, table_name: str)
    response = contact_tote_sys_db(mock_conn, 2, ANY, ANY)
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
def test_contact_tote_sys_db_returns_list_of_rows(test_lists):
    # Arrange
    mock_returned_list_1 = test_lists[0]
    # mock_returned_list_1 looks like:
    # [ 
    # [20496, 'SALE', 14504, None, datetime(2025, 6, 4, 8, 58, 10, 6000), datetime(2025, 6, 4, 8, 58, 10, 6000)],
    # [20497, 'SALE', 14505, None, datetime(2025, 6, 4, 9, 26, 9, 972000), datetime(2025, 6, 4, 9, 26, 9, 972000)],
    # [20498, 'SALE', 14506, None, datetime(2025, 6, 4, 9, 29, 10, 166000), datetime(2025, 6, 4, 9, 29, 10, 166000)]
    # ]

    mock_conn = Mock()
    mock_conn.run.return_value = mock_returned_list_1

    # Act
    response = contact_tote_sys_db(mock_conn, 1, ANY, ANY)
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


