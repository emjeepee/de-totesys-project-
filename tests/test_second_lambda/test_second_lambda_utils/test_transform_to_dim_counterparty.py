import pytest

from datetime import datetime

from src.second_lambda.second_lambda_utils.transform_to_dim_counterparty import transform_to_dim_counterparty




@pytest.fixture
def general_setup():

    # typical design table:
    cp_table = [
        {'counterparty_id': 20, 'counterparty_legal_name': 'Yost, Watsica and Mann', 'legal_address_id': 30, 'commercial_contact': 'Sophie Konopelski', 'delivery_contact': 'Janie Doyle', 'created_at': datetime(2022, 11, 3, 14, 20, 51, 563000), 'last_updated':datetime(2022, 11, 3, 14, 20, 51, 563000)},
        {'counterparty_id': 21, 'counterparty_legal_name': 'Ronald McDonald',        'legal_address_id': 31, 'commercial_contact': 'David Brent',       'delivery_contact': 'Bob Doyle',   'created_at': datetime(2022, 12, 3, 14, 20, 51, 563000), 'last_updated':datetime(2022, 12, 3, 14, 20, 51, 563000)},
        {'counterparty_id': 22, 'counterparty_legal_name': 'Larry Sanders',          'legal_address_id': 32, 'commercial_contact': 'Katie Currick',     'delivery_contact': 'Susan Doyle', 'created_at': datetime(2023, 1, 3, 14, 20, 51, 563000), 'last_updated':datetime(2023, 1, 3, 14, 20, 51, 563000)}
               ]

    expected = [
        {'counterparty_id': 20, 'counterparty_legal_name': 'Yost, Watsica and Mann', "counterparty_legal_address_line_1": '0336 Ruthe Heights', "counterparty_legal_address_line_2": None, "counterparty_legal_district": 'Buckinghamshire', "counterparty_legal_city": 'Lake Myrlfurt', "counterparty_legal_postal_code": '94545-4284', "counterparty_legal_country": 'Falkland Islands (Malvinas)', "counterparty_legal_phone_number": '1083 286132'  },
        {'counterparty_id': 21, 'counterparty_legal_name': 'Ronald McDonald',        "counterparty_legal_address_line_1": '3 Addison Grove',    "counterparty_legal_address_line_2": None, "counterparty_legal_district": 'Pumpkinshire',    "counterparty_legal_city": 'Whitehaven',    "counterparty_legal_postal_code": '94545-5284', "counterparty_legal_country": 'Narnia',                      "counterparty_legal_phone_number": '1083 123456'  },
        {'counterparty_id': 22, 'counterparty_legal_name': 'Larry Sanders',          "counterparty_legal_address_line_1": '4 Fucia Avenue',     "counterparty_legal_address_line_2": None, "counterparty_legal_district": 'Turnipshire',     "counterparty_legal_city": 'Reading',       "counterparty_legal_postal_code": '94545-6284', "counterparty_legal_country": 'Oceania',                     "counterparty_legal_phone_number": '1083 789123'  }
               ]
    
    add_tbl = [
            {'address_id': 30, 'address_line_1': '0336 Ruthe Heights', 'address_line_2': None, 'district': 'Buckinghamshire', 'city': 'Lake Myrlfurt', 'postal_code': '94545-4284', 'country': 'Falkland Islands (Malvinas)', 'phone': '1083 286132', 'created_at': datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2022, 11, 3, 14, 20, 49, 962000)},
            {'address_id': 31, 'address_line_1': '3 Addison Grove',    'address_line_2': None, 'district': 'Pumpkinshire',    'city': 'Whitehaven',    'postal_code': '94545-5284', 'country': 'Narnia',                      'phone': '1083 123456', 'created_at': datetime(2022, 12, 3, 14, 20, 49, 962000), 'last_updated': datetime(2022, 12, 3, 14, 20, 49, 962000)},
            {'address_id': 32, 'address_line_1': '4 Fucia Avenue',     'address_line_2': None, 'district': 'Turnipshire',     'city': 'Reading',       'postal_code': '94545-6284', 'country': 'Oceania',                     'phone': '1083 789123', 'created_at': datetime(2023, 1, 3, 14, 20, 49, 962000),  'last_updated': datetime(2023, 1, 3, 14, 20, 49, 962000)}
              ]


    yield cp_table, expected, add_tbl




# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    (cp_table, expected, add_tbl) = general_setup

    expected = list

    # Act
    # transform_to_dim_location(address_data)
    response = transform_to_dim_counterparty(cp_table, add_tbl)
    
    result = type(response)
    # result = None

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_returns_correct_list_with_correct_key_value_pairs(general_setup):
    # Arrange
    (cp_table, expected, add_tbl) = general_setup


    # Act
    # transform_to_dim_location(address_data)
    result = transform_to_dim_counterparty(cp_table, add_tbl)
    # result = None

    # Assert
    assert result == expected


