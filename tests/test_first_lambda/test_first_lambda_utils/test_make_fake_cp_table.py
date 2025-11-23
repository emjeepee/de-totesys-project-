from datetime import datetime, date, time, timedelta


from src.first_lambda.first_lambda_utils.make_fake_cp_table import make_fake_cp_table



def test_returns_a_list():
    # Arrange:
    expected_fail = str
    expected = list

    # Act
    response = make_fake_cp_table()
    result = type(response)

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected


def test_returns_list_of_ten_dicts():
    # Arrange:
    expected_fail = 9
    expected = 10

    # Act:
    response = make_fake_cp_table()
    result = len(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected 



def test_returns_correct_list():
    # Arrange:
    base_day = datetime(2025, 11, 13) 
    expected_fail = {}
    expected = {
        'counterparty_id': 3, 
        'counterparty_legal_name': 'Armstrong Inc', 
        'legal_address_id': 3, 
        'commercial_contact': 'Jane Wiza', 
        'delivery_contact': 'Myra Kovacek', 
        'created_at': base_day, 
        'last_updated': base_day
                }

    # Act
    response = make_fake_cp_table()
    result = response[2]
    

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected