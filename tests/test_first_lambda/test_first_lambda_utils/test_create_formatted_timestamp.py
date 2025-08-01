from datetime import datetime, timezone
import re
from src.first_lambda.first_lambda_utils.create_formatted_timestamp import create_formatted_timestamp
from unittest.mock import Mock, patch, ANY


def test_create_formatted_timestamp_returns_correctly_formatted_string():
    # Arrange
    # expect string returned by function to be of this form:
    # "2025-08-01_13-45-20".
    # Make a raw string of regex:
    pattern = r"^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}$"

    # Act
    # make timestamp:
    ts = create_formatted_timestamp()
    
    # use re see if timestamp created by 
    # create_formatted_timestamp() 
    # matches the regex pattern:
    result = bool(re.match(pattern, ts))


    # Assert
    assert result






def test_create_formatted_timestamp_returns_correct_time():
    # Arrange

    # make a datetime object and extract
    # information from it: 
    now_dt_object = datetime.now(timezone.utc)
    
    # Omit seconds because that could 
    # make the test fail:
    expected_year = now_dt_object.year
    expected_month = now_dt_object.month
    expected_day = now_dt_object.day
    expected_hour = now_dt_object.hour
    expected_minute = now_dt_object.minute
    

    # make timestamp and extract data 
    # from it (ts will look like this:
    # '2025-06-13_13-13-13'):
    ts = create_formatted_timestamp()
    result_year = int(ts[0:4])
    result_month = int(ts[5:7])
    result_day = int(ts[8:10])
    result_hour = int(ts[11:13])
    result_minute = int(ts[14:16])
    
    # Act
    # make timestamp:
    ts = create_formatted_timestamp()

    # Assert
    assert result_year == expected_year 
    assert result_month == expected_month
    assert result_day == expected_day
    assert result_hour == expected_hour
    assert result_minute == expected_minute
    
    
    
    
