import pytest

from unittest.mock import Mock, patch

from src.second_lambda.second_lambda_utils.make_dictionary import make_dictionary


@pytest.fixture(scope="function")
def setup():
    key_pairs = [
    ("counterparty_legal_address_line_1", "address_line_1"),
    ("counterparty_legal_address_line_2", "address_line_2"),
    ("counterparty_legal_district", "district"),
                ]
    
    src_dict = {"address_line_1": "1", "address_line_2": "2", "district": "3"}

    yield key_pairs, src_dict



def test_returns_dict(setup):
    # arrange:
    key_pairs, src_dict = setup
    expected_fail = "earthworm"
    expected = dict
   

    # act:
    response = make_dictionary(src_dict, key_pairs)
    result = type(response)

    # assert:
    assert result == expected
    



def test_returns_dict_with_correct_number_of_keys(setup):
    # arrange:
    key_pairs, src_dict = setup
    expected_fail = "hot barbecued duck on cold rice"
    expected = 3
   

    # act:
    response = make_dictionary(src_dict, key_pairs)
    result = len(response)

    # assert:
    assert result == expected


def test_returns_dict_with_correct_key_value_pairs(setup):
    # arrange:
    key_pairs, src_dict = setup

    expected_key_1 = "counterparty_legal_address_line_1"
    expected_key_2 = "counterparty_legal_address_line_2"
    expected_key_3 = "counterparty_legal_district"
    
    expected_val_1 = src_dict["address_line_1"]
    expected_val_2 = src_dict["address_line_2"]
    expected_val_3 = src_dict["district"]
   

    # act:
    new_dict = make_dictionary(src_dict, key_pairs)
    keys_list = list(new_dict.keys())
    
    result_val_1 = new_dict["counterparty_legal_address_line_1"]
    result_val_2 = new_dict["counterparty_legal_address_line_2"]
    result_val_3 = new_dict["counterparty_legal_district"]

    # assert:
    assert keys_list[0] == expected_key_1
    assert keys_list[1] == expected_key_2
    assert keys_list[2] == expected_key_3

    assert result_val_1 == expected_val_1
    assert result_val_2 == expected_val_2
    assert result_val_3 == expected_val_3