import pytest
import duckdb
from unittest.mock import Mock, patch

from src.third_lambda.third_lambda_utils.make_list_of_formatted_row_values import make_list_of_formatted_row_values
from src.third_lambda.third_lambda_utils.format_value import format_value


# 
@pytest.fixture(scope="function")
def setup():
    row = (1,  'xxx',   75.50,  None,  True )

    yield row



def test_returns_a_list(setup):

    # Arrange:
    row = setup
    expected = list
    expected_fail = 'list'

    # Act:
    response = make_list_of_formatted_row_values(row)
    result = type(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail 
    assert result == expected 





def test_calls_format_value_correctly(setup):
    # Arrange:
    row = setup        

    with patch("src.third_lambda.third_lambda_utils.make_list_of_formatted_row_values.format_value") as mock_fv:
        mock_fv.side_effect = lambda x: f"val{x}"

        # Act:
        result = make_list_of_formatted_row_values(row)

        # Assert:
        assert mock_fv.call_count == len(row)

        # check function called at 
        # least once with argument 
        # 1 then  'xxx' then 75.50
        # then  None and, finally, 
        # True:
        # ensure test can fail:
        # mock_fv.assert_any_call('fail_arg')
        mock_fv.assert_any_call(1)
        mock_fv.assert_any_call('xxx')
        mock_fv.assert_any_call(75.50)
        mock_fv.assert_any_call(None)
        mock_fv.assert_any_call(True)

        # check the result:
        # ensure test can fail:
        # assert result == ["1", "2", "3"]
        assert result == ["val1", "valxxx", "val75.5", "valNone", "valTrue"]


