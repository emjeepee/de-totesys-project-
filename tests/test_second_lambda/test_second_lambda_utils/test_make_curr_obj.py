import pytest

from currency_codes import get_currency_by_code, Currency, CurrencyNotFoundError

from src.second_lambda.second_lambda_utils.make_curr_obj import make_curr_obj



def test_returns_Currency_object():
    # Arrange:
    # Mock a row dictionary of the 
    # preprocessed currency dimension
    # table:
    mock_curr_row = {'currency_id': 3, 'currency_code': 'EUR'}
    expected = Currency

    # Act: 
    response = make_curr_obj(mock_curr_row)
    result = type(response) 
    # result = None

    # Assert:
    assert result == expected





def test_raises_RuntimeError():
    # Arrange and act:
    # Mock a row dictionary of the 
    # preprocessed currency dimension
    # table that has a dodgy currency 
    # code:
    mock_curr_row = {'currency_id': 3, 'currency_code': 'Monopoly'}

    with pytest.raises(RuntimeError):
        # return
        make_curr_obj(mock_curr_row)










