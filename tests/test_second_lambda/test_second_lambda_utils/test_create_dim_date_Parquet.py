import pytest
import duckdb
import tempfile

from unittest.mock import patch
from datetime import datetime 
from io import BytesIO


from src.second_lambda.second_lambda_utils.create_dim_date_Parquet import create_dim_date_Parquet



@pytest.fixture
def general_setup():
        start_date = datetime(24, 1, 1)
        # Make a mock version of the Python list
        # that make_dim_date_ptython() returns:
        mock_dim_date_py = [{}, {}, {}]

        # Make a mock Parquet file version 
        # of the file that convert_to_Parquet()
        # returns:
        mock_Parquet = 'mock_Parquet_file'
        
        ts = "2025-08-14_12-33-27" 
        nor = 3

        dim_date_key = f"dim_date/{ts}.parquet" # "2025-08-11_15-42-10/dim_date.parquet"

        yield start_date, mock_dim_date_py, mock_Parquet, ts, nor, dim_date_key



# @pytest.mark.skip
def test_returns_a_buffer(general_setup):
    # Arrange:
    ( start_date, mock_dim_date_py, mock_Parquet, ts, nor, dim_date_key ) = general_setup
    expected = list

    # Act:
    result = create_dim_date_Parquet(start_date, ts, nor)

    # Assert:
    assert isinstance(result, BytesIO)
    


# @pytest.mark.skip
def test_returned_buffer_contains_parquet_file(general_setup):
    # Arrange:
    ( start_date, mock_dim_date_py, mock_Parquet, ts, nor, dim_date_key ) = general_setup
    conn = duckdb.connect()
    expected = list

    # Act:
    response = create_dim_date_Parquet(start_date, ts, nor)
    response.seek(0)


    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        tmp.write(response.getvalue())  # write BytesIO to file
        tmp.flush()
        # NOTE fetchall() returns a 
        # list of tuples, not the 
        # original list of 
        # dictionaries:
        output = conn.execute("SELECT * FROM parquet_scan(?)", [tmp.name]).fetchall()
        result = type(output)
        
        assert result == expected
        assert len(output) == 3
        
    




def test_calls_functions_correctly(general_setup):
    # Arrange:
    (start_date, 
    mock_dim_date_py, 
    mock_Parquet, 
    ts, 
    nor, 
    dim_date_key)  = general_setup

    # Act:
    with patch('src.second_lambda.second_lambda_utils.create_dim_date_Parquet.make_dim_date_python') as mddp, \
         patch('src.second_lambda.second_lambda_utils.create_dim_date_Parquet.convert_to_parquet') as ctp: 

        mddp.return_value = mock_dim_date_py            
        ctp.return_value  = mock_Parquet    

        response = create_dim_date_Parquet(start_date, ts, nor)

        print("\n\n\nmddp calls =", mddp.call_args_list)
        print("ctp calls =", ctp.call_args_list)

        mddp.assert_called_once_with(start_date, nor)
        ctp.assert_called_once_with(mock_dim_date_py, 'dim_date')        
