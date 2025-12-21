import pytest


from unittest.mock import Mock, patch, call
from src.second_lambda.second_lambda_utils.transform_to_dim_currency import transform_to_dim_currency



@pytest.fixture
def general_setup():

    mock_pp_curr_table = [
        {'currency_id': 3, 'currency_code': 'EUR'},
        {'currency_id': 4, 'currency_code': 'GBP'},
        {'currency_id': 5, 'currency_code': 'USD'}
                          ]
    
    
    mock_preproc_table = [
        {'currency_id': 3, 'currency_code': 'EUR'},
        {'currency_id': 4, 'currency_code': 'GBP'},
        {'currency_id': 5, 'currency_code': 'USD'}
                         ]

    
    final_table = [
        {'currency_id': 3, 'currency_code': 'EUR', 'currency_name': 'Euro'},
        {'currency_id': 4, 'currency_code': 'GBP', 'currency_name': 'Pound Sterling'},
        {'currency_id': 5, 'currency_code': 'USD',  'currency_name': 'US Dollar'}
                         ]




    yield mock_pp_curr_table, mock_preproc_table, final_table




# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    mock_pp_curr_table, expected, final_table = general_setup

    expected = list

    # Act
    # transform_to_dim_location(address_data)
    response = transform_to_dim_currency(mock_pp_curr_table)
    result = type(response)

    # Assert
    assert result == expected





# @pytest.mark.skip
def test_calls_functions_correctly(general_setup):
    # arrange:
    mock_pp_curr_table, mock_preproc_table, final_table = general_setup

    with patch('src.second_lambda.second_lambda_utils.transform_to_dim_currency.preprocess_dim_tables') as mock_pdt, \
         patch ('src.second_lambda.second_lambda_utils.transform_to_dim_currency.make_curr_obj') as mock_mco: \

        mock_pdt.return_value = mock_preproc_table

        # act:
        response = transform_to_dim_currency(mock_pp_curr_table)
        

        # assert:
        mock_pdt.assert_called_once_with(mock_pp_curr_table,
                                         ["created_at", "last_updated"])
        
        assert mock_mco.call_count == 3
        mock_mco.assert_has_calls(
            [call(mock_preproc_table[0]),
             call(mock_preproc_table[1]),
             call(mock_preproc_table[2])]
                                 )





def test_returns_correct_list(general_setup):
    # arrange:
    mock_pp_curr_table, mock_preproc_table, final_table = general_setup

    # act:
    result = transform_to_dim_currency(mock_pp_curr_table)
    assert result == final_table
