import pytest
import calendar


from datetime import datetime, timedelta
from io import BytesIO
from unittest.mock import Mock, patch

from src.third_lambda.third_lambda_utils.preprocess_buffer_1 import preprocess_buffer_1
from zz_to_dump.convert_to_parquetOLD import convert_to_parquet





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

    pq_buff = convert_to_parquet(dim_date)

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

    yield dim_date, pq_buff, cols, cols_str




def test_calls_correct_methods(setup):
    # Arrange:
    dim_date, pq_buff, cols, cols_str = setup
    mock_row_list = dim_date

    # Act:
    result = preprocess_buffer_1(pq_buff)

    with patch("src.third_lambda.third_lambda_utils.preprocess_buffer_1.tempfile.NamedTemporaryFile") as mock_ntf:
        with patch("src.third_lambda.third_lambda_utils.preprocess_buffer_1.duckdb.connect") as mock_conn:
            # Arrange:
            # Mock the return value
            # of tempfile.NamedTemporaryFile():
            mock_tmp_file = Mock()
            mock_tmp_file.write = Mock()
            mock_tmp_file.flush = Mock()
            mock_ntf.return_value = mock_tmp_file

            # Mock the duckdb.connect method:
            # conn = Mock()
            # conn.execute = Mock()
            # mock_conn.return_value = conn


            # Assert
            # Ensure test can fail:
            assert mock_tmp_file.write.called_once_with('pq_buff.getvalue()')
            # assert mock_tmp_file.write.called_once_with(pq_buff.getvalue())



            # test this: 
            # row_list = arr_table.to_pylist()
            # cols = list(row_list[0].keys())
            # cols_str = ", ".join(cols)  
            # [row_list, cols, cols_str]
           


@pytest.mark.skip
def test_returns_correct_list(setup):
    # Arrange:
    dim_date, pq_buff, cols, cols_str = setup

    # Act:
    result = preprocess_buffer_1(pq_buff, dim_date)

    # Ensure test can fail:
    assert result == ['dim_date, cols, cols_str']
    # assert result == [dim_date, cols, cols_str]


    
    



# ============================================================
# # OLD CODE    
# def test_calls_methods_of_con(setup):
#     # Arrange:

#     dim_date, pq_buff = setup
#     # Mock the buffer
#     mock_buffer = Mock()
    
#     # Mock return value of 
#     # duckDB.connect():
#     mock_con    = Mock()
#     # mock this:
#     # df = con.execute("SELECT * FROM read_parquet(?)", [buffer]).fetchall()       
#     mock_exec_return = Mock()
#     mock_con.execute.return_value = mock_exec_return
#     mock_exec_return.fetchall.return_value = [('aaa', 'xxx', 'yyy')]

#     mock_con.description = ['aaa', 'bbb']

    
#     with patch("src.third_lambda.third_lambda_utils.preprocess_buffer.duckdb.connect") as mock_connect:
#         # Act:
#         mock_connect.return_value = mock_con

#         preprocess_buffer(mock_buffer, dim_date)

#         # Ensure test is capable of failing:
#         # mock_connect.assert_called_once_with(database=':xxx:')
#         mock_connect.assert_called_once_with(database=':memory:')

#         # Ensure test is capable of failing:
#         # mock_con.execute.assert_called_once_with("xxx", [mock_buffer])        
#         mock_con.execute.assert_called_once_with("SELECT * FROM read_parquet(?)", [mock_buffer])        