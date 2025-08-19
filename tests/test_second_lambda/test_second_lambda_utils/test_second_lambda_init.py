import pytest

from src.second_lambda.second_lambda_utils import second_lambda_init


# Need to:
# 1) test second_lambda_init() returns a dict
# 2) test second_lambda_init() returns a dict
#    with the correct keys and values 




def test_returns_a_dict():
    # Arrange:
    expected = dict

    # Act:
    response = second_lambda_init()
    result = type(response) 

    # Assert:
    assert result == expected

    






def test_xxx():
    # Arrange:

    # Act:

    # Assert:

    pass