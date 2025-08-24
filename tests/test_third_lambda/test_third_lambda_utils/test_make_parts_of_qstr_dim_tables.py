import pytest

from src.third_lambda.third_lambda_utils.make_parts_of_qstr_dim_tables import make_parts_of_qstr_dim_tables

@pytest.fixture(scope="function")
def general_setup():
    cols = ['xxx', 'yyy', 'zzz']
    vals = ['1', 'NULL', 'turnip']

    col_val_pairs = 'xxx = 1, yyy = NULL, zzz = turnip, ;'
    cols_str = '(xxx, yyy, zzz, )'
    vals_str = '(1, NULL, turnip, )'

    yield cols, vals, col_val_pairs, cols_str, vals_str 



def test_returns_list(general_setup):
    # Arrange:
    (cols, vals, col_val_pairs, cols_str, vals_str ) = general_setup
    expected = list

    # Act:
    # make_parts_of_qstr_dim_tables(cols, vals)
    response = make_parts_of_qstr_dim_tables(cols, vals)
    result = type(response)
    # result = None

    # Assert:
    assert result == expected


def test_returns_correct_list(general_setup):
    # Arrange:
    (cols, vals, col_val_pairs, cols_str, vals_str ) = general_setup
    expected_0 = cols_str
    expected_1 = vals_str
    expected_2 = col_val_pairs

    # Act:
    # make_parts_of_qstr_dim_tables(cols, vals)
    response = make_parts_of_qstr_dim_tables(cols, vals)
    result_0 = response[0]
    result_1 = response[1]
    result_2 = response[2]
    # result_0 = None
    # result_1 = None
    # result_2 = None

    # Assert:
    assert result_0 == expected_0
    assert result_1 == expected_1
    assert result_2 == expected_2

