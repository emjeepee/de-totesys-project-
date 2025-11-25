import pytest

from decimal import Decimal
from datetime import datetime

from src.first_lambda.first_lambda_utils.make_data_json_safe import make_data_json_safe



@pytest.fixture(scope="function")
def setup():

    dict_0 = {"col_1": Decimal('2.47'), "col_2": datetime(2025, 11, 13) ,  "col_3": 2}
    dict_1 = {"col_1": Decimal('3.14'), "col_2": datetime(2025, 11, 23),  "col_3": "a string"}

    mock_table = [
        dict_0, 
        dict_1 
                 ]

    yield dict_0, dict_1, mock_table




def test_returns_list(setup):
    # arrange:
    dict_0, dict_1, mock_table = setup
    expected = list
    expected_fail = str

    # act:
    reponse = make_data_json_safe(mock_table)
    result = type(reponse)

    # assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected


            

def test_returns_different_list(setup):
    # arrange:
    dict_0, dict_1, mock_table = setup
    expected = mock_table
    expected_fail = "spam, spam, egg and spam"

    # act:
    result = make_data_json_safe(mock_table)

    # assert:
    # ensure test can fail:
    # assert result is expected_fail
    assert result is not expected
    
    

def test_returns_list_whose_internals_are_different(setup):
    # arrange:
    dict_0, dict_1, mock_table = setup
    expected_fail = "spam, spam, egg and spam"

    # act:
    response = make_data_json_safe(mock_table)
    result_0 = response[0]
    result_1 = response[1]

    # assert:
    # ensure test can fail:
    # assert result_0 is expected_fail
    # assert result_1 is expected_fail
    assert result_0 is not dict_0
    assert result_1 is not dict_1


def test_returns_correct_list(setup):
    # arrange:
    dict_0, dict_1, mock_table = setup
    expected_fail = "spam, spam, egg and spam"

    # act:
    response = make_data_json_safe(mock_table)
    res_dict_0 = response[0]
    res_dict_1 = response[1]
    result_0 = res_dict_0["col_1"]
    result_1 = res_dict_0["col_2"]
    result_2 = res_dict_0["col_3"]
    result_3 = res_dict_1["col_1"]
    result_4 = res_dict_1["col_2"]
    result_5 = res_dict_1["col_3"]

    # assert:
    # ensure test can fail:
    # assert result_0 is expected_fail
    assert result_0 == '2.47'
    assert result_1 == "2025-11-13T00:00:00"
    assert result_2 == 2
    assert result_3 == '3.14'
    assert result_4 == "2025-11-23T00:00:00"
    assert result_5 == 'a string'