import pytest

from unittest.mock import Mock, patch, ANY

from src.third_lambda.third_lambda_utils.make_query_for_one_row_dim_table import make_query_for_one_row_dim_table 

@pytest.fixture(scope="function")
def general_setup():
    # make_query_for_one_row_dim_table(table_name: str, pk_col: str, cols: list, vals: list)

    table_name = 'design'

    pk_col = 'design_id'

    cols = ['design_id', 'xxx', 'yyy',  'zzz']
    vals = [13,          '1',   'NULL', 'sausages']


    exp_cols_str = '(design_id, xxx, yyy, zzz)'
    exp_vals_str = "(13, '1', NULL, 'sausages')"

    exp_cv_pairs = "design_id = 13, xxx = '1', yyy = NULL, zzz = 'sausages';"

    yield table_name, pk_col, cols, vals, exp_cols_str, exp_vals_str, exp_cv_pairs 



# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange:
    (table_name, pk_col, cols, vals, exp_cols_str, exp_vals_str, exp_cv_pairs) = general_setup
    expected = str

    # Act:
    # make_query_for_one_row_dim_table(table_name, pk_col, cols, vals)
    response = make_query_for_one_row_dim_table(table_name, pk_col, cols, vals)
    result = type(response)
    # result = None

    # Assert:
    assert result == expected




# @pytest.mark.skip
def test_calls_internal_function_correctly(general_setup):
    # Arrange:
    (table_name, pk_col, cols, vals, exp_cols_str, exp_vals_str, exp_cv_pairs) = general_setup

    # Act and assert:
    # make_parts_of_a_query_string(cols, vals) # [cols_str, vals_str]
    with patch('src.third_lambda.third_lambda_utils.make_query_for_one_row_dim_table.make_parts_of_a_query_string') as mock_mpoaqs:
        # make_parts_of_a_query_string(cols, vals) # [cols_str, vals_str]
        result = make_query_for_one_row_dim_table(table_name, pk_col, cols, vals)
        mock_mpoaqs.assert_called_once_with(cols, vals)




# @pytest.mark.skip
def test_returns_correct_query_string(general_setup):
    # Arrange:
    (table_name, pk_col, cols, vals, exp_cols_str, exp_vals_str, exp_cv_pairs) = general_setup

    # The query string the 
    # function under test 
    # should make:
    # f'INSERT INTO {table_name} {exp_cols_str}' 
    # f'VALUES {exp_vals_str}' 
    # f'ON CONFLICT {pk_col}' 
    # f'DO UPDATE SET {col_val_pairs};'    


    expected = f"INSERT INTO {table_name} {exp_cols_str} VALUES {exp_vals_str} ON CONFLICT {pk_col} DO UPDATE SET {exp_cv_pairs}"


    # Act:
    result = make_query_for_one_row_dim_table(table_name, pk_col, cols, vals)
    # result = None

    # Assert:
    assert result == expected

