import pytest

from unittest.mock import Mock, patch, ANY

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

    yield mock_so_table_dict, mock_table_name, mock_row_list



def test_returns_a_dict(test_vars):
    # arrange:
    mock_so_table_dict, mock_table_name, mock_row_list = test_vars
    expected = dict

    # act:
    response = clean_data(mock_table_name, mock_so_table_dict)
    result = type(response)

    # assert:
    assert result == expected



def test_returns_a_dict_with_correct_key_and_value(test_vars):
    # arrange:
    mock_so_table_dict, mock_table_name, mock_row_list = test_vars
    expected_key = 'sales_order'
    expected_value = mock_row_list

    # act:
    response = clean_data(mock_table_name, mock_so_table_dict)
    for k, v in response.items():
        result_key = k
        result_value = v

    # assert:
    assert result_key == expected_key
    assert result_value == expected_value




def test_calls_make_data_json_safe_correctly(test_vars):
    # arrange:
    mock_so_table_dict, mock_table_name, mock_row_list = test_vars

    # act:
    with patch('src.first_lambda.first_lambda_utils.clean_data.make_data_json_safe') as mock_mdjs:
        response = clean_data(mock_table_name, mock_so_table_dict)

        mock_mdjs.assert_called_once_with(mock_row_list)
