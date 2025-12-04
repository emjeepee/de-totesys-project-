
import calendar
import pytest

from datetime import timedelta, datetime

from src.second_lambda.second_lambda_utils.make_dim_date_python import make_dim_date_python



@pytest.fixture(scope="function")
def general_setup():
    start_date = datetime(24, 1, 1)
    loop_range = 3

    yield start_date, loop_range



def test_returns_list(general_setup):
    # Arrange
    (start_date, loop_range) = general_setup
    expected = list

    # Act
    response = make_dim_date_python(start_date, loop_range)
    result = type(response)

    # Assert
    assert result == expected



def test_returns_list_of_correct_number_of_dicts(general_setup):
    # Arrange
    (start_date, loop_range) = general_setup
    expected = 3

    # Act
    response = make_dim_date_python(start_date, loop_range)
    dict_counter = 0
    for item in response:
        if type(item) == dict:
            dict_counter += 1

    result = dict_counter

    # Assert
    assert result == expected




def test_returned_list_contains_dicts_of_correct_keys(general_setup):
    # Arrange  
    (start_date, loop_range) = general_setup      
    expected_0 = ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"] 
    expected_1 = ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"] 
    expected_2 = ["date_id", "year", "month", "day", "day_of_week", "day_name", "month_name", "quarter"] 

    # Act
    response = make_dim_date_python(start_date, loop_range)
    result_0 = list(response[0].keys())
    result_1 = list(response[1].keys())
    result_2 = list(response[2].keys())

    # Assert:
    assert result_0 == expected_0
    assert result_1 == expected_1
    assert result_2 == expected_2




def test_returned_list_contains_dicts_of_correct_values(general_setup):
    # Arrange:  
    (start_date, loop_range) = general_setup      

    third_day_dt = start_date + timedelta(days=2)

    expected_date_id = third_day_dt.date()
    expected_year = third_day_dt.year
    expected_month = third_day_dt.month
    expected_day = third_day_dt.day
    expected_day_of_week = third_day_dt.weekday() + 1
    expected_day_name = calendar.day_name[third_day_dt.weekday()]
    expected_month_name = calendar.month_name[third_day_dt.month]
    expected_quarter = (third_day_dt.month - 1) // 3 + 1



    # Act:
    response = make_dim_date_python(start_date, loop_range)

    result_date_id = response[2]["date_id"]
    result_year = response[2]["year"]
    result_month = response[2]["month"]
    result_day = response[2]["day"]
    result_day_of_week = response[2]["day_of_week"]
    result_day_name = response[2]["day_name"]
    result_month_name = response[2]["month_name"]
    result_quarter = response[2]["quarter"]
    
    # Assert:
    assert result_date_id == expected_date_id
    assert result_year == expected_year
    assert result_month == expected_month
    assert result_day == expected_day
    assert result_day_of_week == expected_day_of_week
    assert result_day_name == expected_day_name
    assert result_month_name == expected_month_name
    assert result_quarter == expected_quarter



#-------------#-------------#-------------#-------------#-------------#-------------#-------------#-------------

    row = {                                                              # 19Aug25 --HAVE CHECKED THESE:
                "date_id": start_date.date(),                            # is datetime.date object. In warehouse, must be SQL date
                "year": start_date.year,                                 # is int. In warehouse, must be SQL INT
                "month": start_date.month,                               # is int (1 for January). In warehouse, must be SQL INT
                "day": start_date.day,                                   # is int. In warehouse, must be SQL INT
                "day_of_week": start_date.weekday() + 1,                 # is int (1 for Monday). In warehouse, must be SQL INT
                "day_name": calendar.day_name[start_date.weekday()],     # is str (eg 'Monday'). In warehouse, must be SQL VARCHAR
                "month_name": calendar.month_name[start_date.month],     # is str (eg 'January'). In warehouse, must be SQL VARCHAR
                "quarter": (start_date.month - 1) // 3 + 1               # is int. In warehouse, must be SQL INT
              }

