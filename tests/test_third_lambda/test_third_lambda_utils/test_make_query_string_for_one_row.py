import pytest

from unittest.mock import Mock, patch

from src.third_lambda.third_lambda_utils.make_query_string_for_one_row import make_query_string_for_one_row



@pytest.fixture(scope="function")
def setup():
    formatted_vals = ['5', '"xyx"', '75.5', '"TRUE"']
    table_name = 'some_table'
    column_str = 'some_id, some_str, some_flt, some_bool'
    
    yield formatted_vals, table_name, column_str 


def test_returns_string(setup):
    # Arrange:
    formatted_vals, table_name, column_str  = setup
    expected_fail = list
    expected = str

    # Act:
    response = make_query_string_for_one_row(formatted_vals, table_name, column_str)    
    result = type(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected



# @pytest.mark.skip
def test_returns_correct_query_string(setup):
    # Arrange:
    formatted_vals, table_name, column_str  = setup
    
    expected_fail = 'INSERT'
    expected = 'INSERT INTO some_table (some_id, some_str, some_flt, some_bool) VALUES (5, "xyx", 75.5, "TRUE");'

    # Act:
    result = make_query_string_for_one_row(formatted_vals, table_name, column_str)    

    # Assert:
    # Ensure test can fail:
    # assert result == expected_fail
    assert result == expected