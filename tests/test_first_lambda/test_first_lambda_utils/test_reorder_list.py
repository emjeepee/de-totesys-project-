import pytest

from unittest.mock import Mock, patch

from src.first_lambda.first_lambda_utils.reorder_list import reorder_list



@pytest.fixture
def setup():

    mock_arg_0 = "address"

    mock_arg_1 = "department"

    mock_arg_2 = "aaaaaaa"

    mock_list =     [
       {'design': [{}, {}, {}]},
       {'sales_order': [{}, {}, {}]},
       {'counterparty': [{}, {}, {}]},
       {'department': [{}, {}, {}]},
       {'staff': [{}, {}, {}]},
       {'address': [{}, {}, {}]},
                    ]
    
    expected_list =     [
       {'department': [{}, {}, {}]}, 
       {'address': [{}, {}, {}]},
       {'design': [{}, {}, {}]},
       {'sales_order': [{}, {}, {}]},
       {'counterparty': [{}, {}, {}]},
       {'staff': [{}, {}, {}]}
                    ]

    yield mock_arg_0, mock_arg_1, mock_list, expected_list 





def test_returns_list(setup):
    # Arrange:
    mock_arg_0, mock_arg_1, mock_list, expected_list = setup

    expected_fail = 'list'
    expected = list

    # Act:
    response = reorder_list(mock_list, mock_arg_0, mock_arg_1)
    result = type(response)


    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected





def test_returns_list_with_correct_items_at_the_beginning(setup):
    # Arrange:
    mock_arg_0, mock_arg_1, mock_list, expected_list = setup

    expected_fail_0 = 'x' 
    expected_fail_1 = 'x'
     
    expected_0 = {'department': [{}, {}, {}]}
    expected_1 = {'address': [{}, {}, {}]}
  
    # Act:
    response = reorder_list(mock_list, mock_arg_0, mock_arg_1 )
    result_0 = response[0]
    result_1 = response[1]
  
    # Assert:
    # ensure test can fail:
    # assert result_0 == expected_fail_0
    # assert result_1 == expected_fail_1

    assert result_0 == expected_0
    assert result_1 == expected_1





def test_returns_correct_list(setup):
    # Arrange:
    mock_arg_0, mock_arg_1, mock_list, expected_list = setup
    expected_fail = "expected_fail"

    # Act:
    result = reorder_list(mock_list, mock_arg_0, mock_arg_1)


    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected_list
