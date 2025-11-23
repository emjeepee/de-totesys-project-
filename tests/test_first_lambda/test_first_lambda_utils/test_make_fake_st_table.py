import pytest

from datetime import datetime, date, time, timedelta

from src.first_lambda.first_lambda_utils.make_fake_st_table import make_fake_st_table



def test_returns_a_list():
    # Arrange:
    expected_fail = str
    expected = list

    # Act
    response = make_fake_st_table()
    result = type(response)

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected




# @pytest.mark.skip
def test_returns_list_of_fifty_dicts():
    # Arrange:
    expected_fail = 42
    expected = 10

    # Act:
    response = make_fake_st_table()
    result = len(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected 



# @pytest.mark.skip
def test_returns_correct_list():
    # Arrange:
    base_day = datetime(2025, 11, 13) 
    td_1_day = timedelta(days=1)
    expected_fail = "expected_fail"
    expected =   {
        'staff_id': 10, 
         'first_name': 'Jazmyn', 
         'last_name': 'Kuhn', 
         'department_id': 3, 
         'email_address': 'jazmyn.kuhn@terrifictotes.com', 
         'created_at': base_day, 
         'last_updated': base_day
         } 


    # Act:
    response = make_fake_st_table()
    result = response[9]

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected
