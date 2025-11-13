import pytest
import duckdb
import calendar

from io import BytesIO
from unittest.mock import Mock, patch
from datetime import datetime, timedelta

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

    yield dim_date, table_name, cols, cols_str




   
    
# @pytest.mark.skip    
def test_calls_functions_correctly(setup):
    """
    Also tests that the function returns the correct list
    """
    # Arrange:
    dim_date, table_name, cols, cols_str = setup

    mock_buff = Mock()
    mock_buff.seek = Mock()
    mock_parquet = Mock()
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
    result = read_parquet_from_buffer(mock_buff, mock_conn)

    # Assert:
    result_type = type(result)
    # Ensure test can fail:    
    # expected_type_fail = 'list'
    # assert result_type == expected_type_fail
    expected_type = list 
    assert result_type == expected_type


    # ensure test can fail:
    # mock_buff.seek.assert_called_once_with('0')
    mock_buff.seek.assert_called_once_with(0)
    # ensure test can fail:
    # mock_conn.execute.assert_called_once_with("SELECT * FROM parquet_scan(?)", ['mock_buff'])
    mock_conn.execute.assert_called_once_with("SELECT * FROM parquet_scan(?)", [mock_buff])
    # ensure test can fail:
    # mock_parquet.fetchall.assert_called_once('xxx')
    mock_parquet.fetchall.assert_called_once()
    # ensure test can fail:
    # expected_fail = ['aaa', 'bbb']
    # assert result == expected_fail
    expected = [cols_str, mock_parquet.fetchall.return_value]
    assert result == expected
