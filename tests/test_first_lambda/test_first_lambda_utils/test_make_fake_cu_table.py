import pytest

from datetime import datetime, date, time, timedelta


from src.first_lambda.first_lambda_utils.make_fake_cu_table import make_fake_cu_table



def test_returns_a_list():
    # Arrange:
    expected = list

    # Act
    response = make_fake_cu_table()
    result = type(response)

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_returns_list_of_three_dicts():
    # Arrange:
    expected = 3

    # Act:
    response = make_fake_cu_table()
    result = len(response)

    # Assert:
    assert result == expected 



# @pytest.mark.skip
def test_returns_correct_list():
    # Arrange:
    base_day = datetime(2025, 11, 13) 
    expected_fail = {}
    expected = {
        'currency_id': 2, 
        'currency_code': 'USD', 
        'created_at': base_day, 
        'last_updated': base_day
              }

    # Act
    response = make_fake_cu_table()
    result = response[1]
    
    # Assert
    assert result == expected