from datetime import datetime, date, time, timedelta
import pytest


from src.first_lambda.first_lambda_utils.make_fake_ad_table import make_fake_ad_table


def test_returns_list():
    # Arrange:
    expected_fail = str
    expected = list

    # Act:
    response = make_fake_ad_table()
    result = type(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected


def test_returns_list_of_ten_dicts():
    # Arrange:
    expected_fail = 9
    expected = 10

    # Act:
    response = make_fake_ad_table()
    result = len(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected    



def test_returns_list_of_correct_dicts():
    # Arrange:
    base_day = datetime(2025, 11, 13) 
    expected_fail = {}
    expected =  {
    'address_id': 8, 
    'address_line_1': '0579 Durgan Common', 
    'address_line_2': None, 
    'district': None, 
    'city': 'Suffolk', 
    'postal_code': '56693-0660', 
    'country': 'United Kingdom', 
    'phone': '8935 157571', 
    'created_at': base_day, 
    'last_updated': base_day
                }

    # Act:
    response = make_fake_ad_table()
    result = response[7]

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected    

