import pytest

from unittest.mock import patch, Mock, ANY

from src.third_lambda.third_lambda_utils.make_query_for_one_row_fact_table import make_query_for_one_row_fact_table 

@pytest.fixture(scope="function")
def general_setup():

    table_name = 'sales_order'

    cols = ['www', 'xxx', 'yyy', 'zzz']
    vals = [13, '1', 'NULL', 'sausages']

    
    exp_cols_str = '(www, xxx, yyy, zzz)'
    exp_vals_str = "(13, '1', NULL, 'sausages')"

    yield table_name, cols, vals, exp_cols_str, exp_vals_str 






# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange:
    (table_name, cols, vals, exp_cols_str, exp_vals_str ) = general_setup
    expected = str

    # Act:
    # make_parts_of_qstr_dim_tables(cols, vals)
    response = make_query_for_one_row_fact_table(table_name, cols, vals)
    result = type(response)
    # result = None

    # Assert:
    assert result == expected




# @pytest.mark.skip
def test_calls_internal_function_correctly(general_setup):
    # Arrange:
    (table_name, cols, vals, exp_cols_str, exp_vals_str ) = general_setup
    
    # Act and assert:
    with patch('src.third_lambda.third_lambda_utils.make_query_for_one_row_fact_table.make_parts_of_a_query_string') as mock_mpoaqs:
        result = make_query_for_one_row_fact_table(table_name, cols, vals)
        mock_mpoaqs.assert_called_once_with(cols, vals)
        









# @pytest.mark.skip
def test_returns_correct_string(general_setup):
    # Arrange:
    (table_name, cols, vals, exp_cols_str, exp_vals_str ) = general_setup
    expected = f"INSERT INTO sales_order {exp_cols_str} VALUES {exp_vals_str};"


    # Act:
    result = make_query_for_one_row_fact_table(table_name, cols, vals)
    # result = None

    # Assert:
    assert result == expected

