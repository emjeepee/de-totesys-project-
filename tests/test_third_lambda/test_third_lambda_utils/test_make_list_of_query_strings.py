import pytest
from unittest.mock import Mock, patch

from src.third_lambda.third_lambda_utils.make_list_of_query_strings import make_list_of_query_strings


# Setup:
@pytest.fixture(scope="function")
def setup():
    rows =         [
          (1,  'xxx',   75.50,   True ),
          (2,  'yyy',   82.00,   False ),
          (3,  'zzz',   69.75,   True )
                 ]

    table_name = 'mock_table'

    column_str = 'some_id, some_thing, some_value, some_bool'

    yield rows, table_name, column_str       





def test_returns_a_list(setup):

    # Arrange:
    rows, table_name, column_str = setup
    # ensure test can fail:
    # expected_fail = 'list'
    expected = list
    

    # Act:
    response = make_list_of_query_strings(rows, table_name, column_str)
    result = type(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail 
    assert result == expected 





def test_calls_first_function_correctly(setup):
    """
    Tests that function make_list_of_query_strings()
    calls make_list_of_formatted_row_values(row)
    the correct number of times and with the correct 
    arg.
    """
    # Arrange:
    rows, table_name, column_str = setup
    # ensure test can fail:

    with patch("src.third_lambda.third_lambda_utils.make_list_of_query_strings.make_list_of_formatted_row_values") as mock_mlofrv, \
        patch("src.third_lambda.third_lambda_utils.make_list_of_query_strings.make_query_string_for_one_row") as mock_mqsfor:
        mock_mlofrv.side_effect = lambda x: x
        mock_mqsfor.side_effect = lambda a, b, c, d: 'x'
    
        result = make_list_of_query_strings(rows, table_name, column_str)

        # ensure test can fail:
        # assert mock_mlofrv.call_count == 1
        assert mock_mlofrv.call_count == len(rows)

        # ensure test can fail:
        # mock_mlofrv.assert_any_call(('xxx'))
        mock_mlofrv.assert_any_call((1,  'xxx',   75.50,   True ))
        mock_mlofrv.assert_any_call((2,  'yyy',   82.00,   False ))
        mock_mlofrv.assert_any_call((3,  'zzz',   69.75,   True ))
        

        # check the result:
        # ensure test can fail:
        # assert result == []
        assert result == ['x', 'x', 'x', 'x']
