import pytest
import calendar

from datetime import datetime, timedelta
from src.second_lambda.second_lambda_utils.make_column_defs import make_column_defs





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
    
    cols_str = 'date_id DATE, ' \
                'year INTEGER, ' \
                'month INTEGER, ' \
               'day INTEGER, ' \
               'day_of_week INTEGER, ' \
               'day_name TEXT, ' \
               'month_name TEXT, ' \
               'quarter INTEGER'

    tmp_path = '/tmp/tmpabcd1234.parquet'

    yield dim_date, cols, cols_str, tmp_path




def test_returns_a_string(setup):
    # Arrange:
    dim_date, cols, cols_str, tmp_path = setup
    # Ensure test can fail:
    # expected = int
    expected = str

    # Act:
    returned = make_column_defs(dim_date)
    result = type(returned)

    # Assert
    assert result == expected 



def test_returns_correct_string(setup):
    # Arrange:
    dim_date, cols, cols_str, tmp_path = setup
    # Ensure test can fail:
    # expected = 'a string to fail the test'
    expected = cols_str

    # Act:
    result = make_column_defs(dim_date)
    
    # Assert
    assert result == expected     
