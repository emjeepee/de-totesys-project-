import pytest

from datetime import datetime, date, time

from src.second_lambda.second_lambda_utils.transform_to_dim_location import transform_to_dim_location




@pytest.fixture
def general_setup():

# typical address table:
    add_tabl = [
        {'address_id': 29, 'address_line_1': '3 Ruthe Heights', 'address_line_2': None, 'district': 'Buckinghamshire', 'city': 'Lake Myrlfurt', 'postal_code': '94545-1234', 'country': 'Falkland Islands (Malvinas)', 'phone': '1083 286132', 'created_at': datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2022, 11, 3, 14, 20, 49, 962000)},
        {'address_id': 30, 'address_line_1': '4 Ruthe Heights', 'address_line_2': None, 'district': 'Buckinghamshire', 'city': 'Lake Myrlfurt', 'postal_code': '94545-1235', 'country': 'Falkland Islands (Malvinas)', 'phone': '1083 286133', 'created_at': datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2022, 11, 3, 14, 20, 49, 962000)},
        {'address_id': 31, 'address_line_1': '5 Ruthe Heights', 'address_line_2': None, 'district': 'Buckinghamshire', 'city': 'Lake Myrlfurt', 'postal_code': '94545-1236', 'country': 'Falkland Islands (Malvinas)', 'phone': '1083 286134', 'created_at': datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2022, 11, 3, 14, 20, 49, 962000)}
              ]
    
    expected = [
        {'location_id': 29, 'address_line_1': '3 Ruthe Heights', 'address_line_2': None, 'district': 'Buckinghamshire', 'city': 'Lake Myrlfurt', 'postal_code': '94545-1234', 'country': 'Falkland Islands (Malvinas)', 'phone': '1083 286132'},
        {'location_id': 30, 'address_line_1': '4 Ruthe Heights', 'address_line_2': None, 'district': 'Buckinghamshire', 'city': 'Lake Myrlfurt', 'postal_code': '94545-1235', 'country': 'Falkland Islands (Malvinas)', 'phone': '1083 286133'},
        {'location_id': 31, 'address_line_1': '5 Ruthe Heights', 'address_line_2': None, 'district': 'Buckinghamshire', 'city': 'Lake Myrlfurt', 'postal_code': '94545-1236', 'country': 'Falkland Islands (Malvinas)', 'phone': '1083 286134'}
              ]
    

    yield add_tabl, expected




# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    (add_tabl, expected) = general_setup

    expected = list

    # Act
    # transform_to_dim_location(address_data)
    response = transform_to_dim_location(add_tabl)
    
    result = type(response)
    # result = None

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_returns_correct_list(general_setup):
    # Arrange
    (add_tabl, expected) = general_setup


    # Act
    # transform_to_dim_location(address_data)
    result = transform_to_dim_location(add_tabl)
    # result = None
   

    # Assert
    assert result == expected


