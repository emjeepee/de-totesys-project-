import pytest


from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock, patch, call 

from src.first_lambda.first_lambda_utils.clean_data import clean_data


@pytest.fixture
def test_vars():

    # Mock what read_table returns
    # when called three times (which 
    # it will be here because the 
    # table_names parameter is set 
    # to a list of three strings):
    mock_so_table_dict =  {'sales_order': [{'sales_dict1_key': 'sales_dict1_value'}, {'sales_dict2_key': 'sales_dict2_value'}]}
    mock_table_name = 'sales_order'
    mock_row_list = [{'sales_dict1_key': 'sales_dict1_value'}, {'sales_dict2_key': 'sales_dict2_value'}]


    dict_0 = {"col_1": Decimal('2.47'), "col_2": datetime(2025, 11, 13) ,  "col_3": 2}
    dict_1 = {"col_1": Decimal('3.14'), "col_2": datetime(2025, 11, 23),  "col_3": "a string"}

    mock_table = [
        dict_0, 
        dict_1 
                 ]
    
    mock_table_dict = {'some_table': mock_table}

    mock_table_name_1 = 'some_table'

    dict_0_changed = {"col_1": '2.47', "col_2": "2025-11-13T00:00:00" ,  "col_3": 2}
    dict_1_changed = {"col_1": '3.14', "col_2": "2025-11-23T00:00:00",  "col_3": "a string"}

    mock_table_changed = [
        dict_0_changed,
        dict_1_changed
        ]



    yield (mock_so_table_dict, 
    mock_table_name, 
    mock_row_list, 
    mock_table, 
    mock_table_dict,
    mock_table_name_1,
    mock_table_changed
            )


def test_returns_a_dict(test_vars):
    # arrange:
    (mock_so_table_dict, 
     mock_table_name, 
     mock_row_list, 
     mock_table, 
     mock_table_dict,
     mock_table_name_1,
     mock_table_changed) = test_vars
    expected = dict

    # act:
    response = clean_data(mock_table_name, mock_so_table_dict)
    result = type(response)

    # assert:
    assert result == expected


def test_calls_change_vals_to_strings_correctly(test_vars):
    # arrange:
    (mock_so_table_dict, 
     mock_table_name, 
     mock_row_list, 
     mock_table, 
     mock_table_dict,
     mock_table_name_1,
     mock_table_changed) = test_vars


    # act:
    with patch('src.first_lambda.first_lambda_utils.clean_data.change_vals_to_strings') as mock_cvts:
        response = clean_data(mock_table_name_1, mock_table_dict)

        # assert:
        # Should call mock_cvts this many times: 
        # (number of rows) x (number of fields in a row)
        assert mock_cvts.call_count == len(mock_table_dict[mock_table_name_1])*3




def test_returns_a_dict_with_correct_key_and_value(test_vars):
    # arrange:
    (mock_so_table_dict, 
     mock_table_name, 
     mock_row_list, 
     mock_table, 
     mock_table_dict,
     mock_table_name_1,
     mock_table_changed) = test_vars

    expected_key = 'some_table'
    expected_value = mock_table_changed

    # act:
    response = clean_data(mock_table_name_1, mock_table_dict)
    for k, v in response.items():
        result_key = k
        result_value = v

    # assert:
    assert result_key == expected_key
    assert result_value == expected_value

        
