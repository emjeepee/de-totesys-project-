import pytest
import calendar

from unittest.mock import patch
from datetime import datetime, timedelta
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
def test_returns_a_list(general_setup):
    # Arrange:
    ( start_date, mock_dim_date_py, mock_Parquet, ts, nor, dim_date_key ) = general_setup
    expected = list
    


    # Act:
    # create_dim_date_Parquet(start_date, timestamp_string: str, num_rows: int)
    # result = None
    reponse = create_dim_date_Parquet(start_date, ts, nor)
    result = type(reponse)



    # Assert:
    assert result == expected 
    pass




# @pytest.mark.skip
def test_returns_correct_Parquet_file(general_setup):
    # Arrange:
    ( start_date, mock_dim_date_py, mock_Parquet, ts, nor, dim_date_key ) = general_setup
    expected_0 = mock_Parquet
    expected_1 = dim_date_key

    # Act:
    with patch('src.second_lambda.second_lambda_utils.create_dim_date_Parquet.make_dim_date_python') as mddp, \
         patch('src.second_lambda.second_lambda_utils.create_dim_date_Parquet.convert_to_parquet') as ctp: \

        mddp.return_value = mock_dim_date_py            
        ctp.return_value  = mock_Parquet

        response = create_dim_date_Parquet(start_date, "2025-08-14_12-33-27", 3)
        result_0 = response[0]
        result_1 = response[1]
        # result_0 = None
        # result_1 = None


        # Assert:
        assert result_0 == expected_0
        assert result_1 == expected_1