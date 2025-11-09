import pytest
import calendar
import duckdb
from io import BytesIO
import tempfile
import os

from unittest.mock import Mock, patch
from datetime import datetime, timedelta

from second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquetNEW
from src.second_lambda.second_lambda_utils.write_parquet_to_buffer import write_parquet_to_buffer



# Set up a mock table:
@pytest.fixture(scope="function")
def setup():
    # Make a mock dimension date 
    # table as a Python list
    # of dictionaries:
    start_date = datetime(24, 1, 1)
    dim_date = []
    for i in range(3):
        row = {                                                          
                "date_id": start_date.date(),                            
                "year": start_date.year,                                 
                "month": start_date.month,                               
                "day": start_date.day,                                   
                "day_of_week": start_date.weekday() + 1,                 
                "day_name": calendar.day_name[start_date.weekday()],     
                "month_name": calendar.month_name[start_date.month],     
                "quarter": (start_date.month - 1) // 3 + 1               
              }

        dim_date.append(row)
        start_date += timedelta(days=1)


    cols = [ "date_id",
                "year", 
                "month",
                "day", 
                "day_of_week", 
                "day_name", 
                "month_name", 
                "quarter"
                ]
    
    cols_str = 'date_id, year, month, day, day_of_the_week, day_name, month_name, quarter'

    tmp_path = '/tmp/tmpabcd1234.parquet'

    yield dim_date, cols, cols_str, tmp_path



def test_calls_duckdb_methods_correctly(setup):
# Arrange:
    dim_date, cols, cols_str, tmp_path = setup

    with patch("src.second_lambda.second_lambda_utils.convert_to_parquetNEW.duckdb.connect") as mock_connect:
        # Create a mock temp file 
        # object that supports 
        # context management
        mock_tmp = Mock()
        # Patch NamedTemporaryFile to return our mock object
        with patch("src.second_lambda.second_lambda_utils.convert_to_parquetNEW.tempfile.NamedTemporaryFile", return_value=mock_tmp):
            # Mock duckdb connection and execute behavior
            mock_conn_obj = Mock()
            mock_connect.return_value = mock_conn_obj
            mock_execute = Mock()
            mock_conn_obj.execute = mock_execute
            mock_close = Mock()
            mock_conn_obj.close = mock_close

            
            mock_tmp.name = tmp_path
            # Explicitly add context manager support:
            mock_tmp.__enter__ = Mock(return_value=mock_tmp)
            mock_tmp.__exit__ = Mock(return_value=None)


            # Act
            result = convert_to_parquetNEW(dim_date)

            # Assert
            # Ensure the test can actually fail:
            # mock_connect.assert_called_once_with(":xxx:")
            mock_connect.assert_called_once_with(":memory:")
            # Ensure the test can actually fail:
            # mock_execute.assert_called_once_with('xxx')
            
            mock_execute.assert_called_once_with("COPY (SELECT * FROM data) TO ? (FORMAT PARQUET)", ['/tmp/tmpabcd1234.parquet'])  # we cann't say 




def test_calls_write_parquet_to_buffer_correctly(setup):
    # Arrange:
    dim_date, cols, cols_str, tmp_path = setup

    # Create a mock temp file 
    mock_tmp = Mock()
    with patch("src.second_lambda.second_lambda_utils.convert_to_parquetNEW.tempfile.NamedTemporaryFile", return_value=mock_tmp):
        mock_tmp.name = tmp_path
        # Explicitly add context manager support:
        mock_tmp.__enter__ = Mock(return_value=mock_tmp)
        mock_tmp.__exit__ = Mock(return_value=None)
        
        with patch("src.second_lambda.second_lambda_utils.convert_to_parquetNEW.write_parquet_to_buffer") as mock_wptb:
            # Act"
            convert_to_parquetNEW(dim_date)
            
            # Assert:
            mock_wptb.assert_called_once_with(tmp_path)

            