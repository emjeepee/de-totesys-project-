import pytest

from datetime import datetime, date, time, timedelta


from src.first_lambda.first_lambda_utils.make_fake_dp_table import make_fake_dp_table



def test_returns_a_list():
    # Arrange:
    expected = list

    # Act
    response = make_fake_dp_table()
    result = type(response)

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_returns_list_of_eight_dicts():
    # Arrange:
    expected = 8

    # Act:
    response = make_fake_dp_table()
    result = len(response)

    # Assert:
    assert result == expected 



# @pytest.mark.skip
def test_returns_correct_list():
    # Arrange:
    base_day = datetime(2025, 11, 13) 
    expected_fail = {}
    expected = {
        'department_id': 2, 
     'department_name': 'Purchasing', 
     'location': 'Manchester', 
     'manager': 'Naomi Lapaglia', 
     'created_at': base_day, 
     'last_updated': base_day
                     }

    # Act
    response = make_fake_dp_table()
    result = response[1]
    
    # Assert
    assert result == expected