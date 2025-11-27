import pytest

from unittest.mock import patch

from currency_codes import get_currency_by_code, Currency, CurrencyNotFoundError

from src.second_lambda.second_lambda_utils.transform_to_dim_currency import transform_to_dim_currency



@pytest.fixture
def general_setup():

    mock_pp_curr_table = [
        {'currency_id': 3, 'currency_code': 'EUR'},
        {'currency_id': 4, 'currency_code': 'GBP'},
        {'currency_id': 5, 'currency_code': 'USD'}
                          ]
    
    # 'currency_name'
    expected = [
        {'currency_id': 3, 'currency_code': 'EUR', 'currency_name': 'Euro'},
        {'currency_id': 4, 'currency_code': 'GBP', 'currency_name': 'Pound Sterling'},
        {'currency_id': 5, 'currency_code': 'USD',  'currency_name': 'US Dollar'}
               ]
    

    yield mock_pp_curr_table, expected




# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    mock_pp_curr_table, expected = general_setup

    expected = list

    # Act
    # transform_to_dim_location(address_data)
    response = transform_to_dim_currency(mock_pp_curr_table)
    
    result = type(response)
    # result = None

    # Assert
    assert result == expected





# @pytest.mark.skip
def test_raises_RuntimeError(general_setup):
    # Arrange
    (mock_pp_curr_table, expected) = general_setup
   

    # Act and assert:
    # transform_to_dim_location(address_data)
    with patch('src.second_lambda.second_lambda_utils.transform_to_dim_currency.make_curr_obj') as mco:
        mco.side_effect = RuntimeError()
        with pytest.raises(RuntimeError):
            # return
            response = transform_to_dim_currency(mock_pp_curr_table)
    
