import pytest

from datetime import datetime, date, time, timedelta


from src.first_lambda.first_lambda_utils.make_fake_de_table import make_fake_de_table



def test_returns_a_list():
    # Arrange:
    expected_fail = str
    expected = list

    # Act
    response = make_fake_de_table()
    result = type(response)

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected




# @pytest.mark.skip
def test_returns_list_of_ten_dicts():
    # Arrange:
    expected_fail = 1
    expected = 10

    # Act:
    response = make_fake_de_table()
    result = len(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected 



# @pytest.mark.skip
def test_returns_correct_list():
    # Arrange:
    base_day = datetime(2025, 11, 13) 
    expected_fail = {}
    expected = {
    'design_id': 5, 
    'created_at': base_day, 
    'design_name': 'plastic',
    'file_location': '/usr/ports',
    'file_name': 'plastic-20241121-t5de.json',
    'last_updated': base_day
                    }

    # Act
    response = make_fake_de_table()
    result = response[4]
    

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected