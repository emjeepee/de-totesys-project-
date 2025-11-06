# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
import calendar


from unittest.mock import Mock, patch
from io import BytesIO
from datetime import datetime, timedelta

from src.second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquet



def test_Parquet_file_is_valid():
    # Arrange: 
    # Make a mock dimension date 
    # table as a Python list
    # of dictionaries:
    start_date = datetime(24, 1, 1)
    dim_date = []
    for i in range(3):
        row = {                                                          # 19Aug25 --HAVE CHECKED THESE:
                "date_id": start_date.date(),                            # is datetime.date object. In warehouse, must be SQL date
                "year": start_date.year,                                 # is int. In warehouse, must be SQL INT
                "month": start_date.month,                               # is int (1 for January). In warehouse, must be SQL INT
                "day": start_date.day,                                   # is int. In warehouse, must be SQL INT
                "day_of_week": start_date.weekday() + 1,                 # is int (1 for Monday). In warehouse, must be SQL INT
                "day_name": calendar.day_name[start_date.weekday()],     # is str (eg 'Monday'). In warehouse, must be SQL VARCHAR
                "month_name": calendar.month_name[start_date.month],     # is str (eg 'January'). In warehouse, must be SQL VARCHAR
                "quarter": (start_date.month - 1) // 3 + 1               # is int. In warehouse, must be SQL INT
              }

        dim_date.append(row)
        start_date += timedelta(days=1)

    mock_buffer = BytesIO(b"mock-parquet-bytes")    

    mock_con = Mock()
    mock_con.execute.return_value = None
    mock_con.register.return_value = None

    with patch("src.second_lambda.second_lambda_utils.convert_to_parquet.duckdb.connect", return_value=mock_con):
        with patch("src.second_lambda.second_lambda_utils.convert_to_parquet.BytesIO", return_value=mock_buffer):
            result = convert_to_parquet(dim_date)

    # Assertions
    mock_con.register.assert_called_once_with('table', dim_date)
    mock_con.execute.assert_called_once_with("COPY table TO buffer (FORMAT PARQUET)")
    assert result == b"mock-parquet-bytes"











# OLD code:
    # # Make a Pandas DataFrame
    # # from the Python list of 
    # # dictionaries:
    # expected_df = pd.DataFrame(dim_date)        

    # # Act: 
    # # Make the Parquet file 
    # # and put it in a buffer:
    # buffer = convert_to_parquet(dim_date)
    # # Go back to the beginning 
    # # of the buffer: 
    # buffer.seek(0)  
    # # Make Pandas dataframe
    # # from Parquet file:
    # df_from_pq = pd.read_parquet(buffer, engine="pyarrow")
    
    # # Assert: 
    # pd.testing.assert_frame_equal(df_from_pq, expected_df)

