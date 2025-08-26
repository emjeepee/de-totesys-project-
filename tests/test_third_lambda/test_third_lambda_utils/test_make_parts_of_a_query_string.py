import pytest


from src.third_lambda.third_lambda_utils.make_parts_of_a_query_string import make_parts_of_a_query_string



@pytest.fixture(scope="function")
def general_setup():

    cols = ['design_id', 'xxx',  'yyy',  'zzz']
    vals = [13,          'TRUE', 'NULL', 'cabbage']

    exp_col_str = '(design_id, xxx, yyy, zzz)'
    exp_val_str = "(13, TRUE, NULL, 'cabbage')"

    yield cols, vals, exp_col_str, exp_val_str



def test_returns_list(general_setup):
    # Arrange:
    (cols, vals, exp_col_str, exp_val_str) = general_setup
    expected = list

    # Act:
    response = make_parts_of_a_query_string(cols, vals)
    result = type(response)
    # result = None

    # Assert:
    assert result == expected




# @pytest.mark.skip
def test_returns_list_with_correct_member_at_index_0(general_setup):
    # Arrange:
    (cols, vals, exp_col_str, exp_val_str) = general_setup
    expected_0 = exp_col_str

    # Act:
    response = make_parts_of_a_query_string(cols, vals)
    result_0 = response[0]
    # result_0 = None


    # Assert:
    assert result_0 == expected_0




# @pytest.mark.skip
def test_returns_list_with_correct_member_at_index_1(general_setup):
    # Arrange:
    (cols, vals, exp_col_str, exp_val_str) = general_setup
    expected_1 = exp_val_str

    # Act:
    response = make_parts_of_a_query_string(cols, vals)
    result_1 = response[1]
    # result_1 = None

    # Assert:
    assert result_1 == expected_1    