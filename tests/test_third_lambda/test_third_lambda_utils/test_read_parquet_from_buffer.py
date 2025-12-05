import pytest


from unittest.mock import Mock, ANY
from datetime import datetime

from src.third_lambda.third_lambda_utils.read_parquet_from_buffer import read_parquet_from_buffer



# Set up a mock table:
@pytest.fixture(scope="function")
def setup():
    # Make a mock dimension date 
    # table as a Python list
    # of dictionaries:
    start_date = datetime(24, 1, 1)
    dim_date = []

    table_name = 'dim_date'

    dim_date.append( {                                                          
                "date_id": 1,
                "year": 'mock_year_1',
                "month": 'mock_month_1',
                "day": 'mock_day_1'
                    })
    
    dim_date.append( {                                                          
                "date_id": 2,
                "year": 'mock_year_2',
                "month": 'mock_month_2',
                "day": 'mock_day_2'
                    })

    dim_date.append( {                                                          
                "date_id": 3,
                "year": 'mock_year_2',
                "month": 'mock_month_2',
                "day": 'mock_day_2'
                    })


    cols = [ "date_id",
                "year", 
                "month",
                "day" 
            ]
    
    cols_str = '"date_id", "year", "month", "day"'

    

    # buffer = BytesIO() # create an empty bytes buffer
    # buffer.write(b"mock_parquet_file") # write bytes obj to buffer
    # data = buffer.getvalue()
    buffer = b"mock parquet bytes"    


    yield dim_date, table_name, cols, cols_str, buffer




   
    
# @pytest.mark.skip    
def test_calls_functions_correctly(setup):
    """
    Also tests that the function returns the correct list
    """
    # Arrange:
    dim_date, table_name, cols, cols_str, buffer = setup

    mock_pq_in_buff = Mock()
    mock_pq_in_buff.seek = Mock()
    mock_pq_in_buff.getvalue = Mock()
    mock_parquet = Mock()
    mock_pq_in_buff.getvalue.return_value = buffer
    mock_parquet.fetchall = Mock()
    mock_parquet.fetchall.return_value = [
    (1, 'mock_year_1', 'mock_month_1', 'mock_day_1'),
    (2, 'mock_year_2', 'mock_month_2', 'mock_day_2'),
    (3, 'mock_year_3', 'mock_month_3', 'mock_day_3'),
                                        ]

    mock_parquet.description = [
                ["date_id"],
                ["year"], 
                ["month"],
                ["day"]]

    mock_conn = Mock()
    mock_conn.execute = Mock()
    mock_conn.execute.return_value = mock_parquet

    # Act:
    result = read_parquet_from_buffer(mock_pq_in_buff, mock_conn)

    # Assert:
    result_type = type(result)
    expected_type = list 
    assert result_type == expected_type

    mock_pq_in_buff.seek.assert_called_once_with(0)
    mock_conn.execute.assert_called_once_with("SELECT * FROM parquet_scan(?)", [ANY])
    mock_parquet.fetchall.assert_called_once()
    expected = [cols_str, mock_parquet.fetchall.return_value]

    assert result == expected