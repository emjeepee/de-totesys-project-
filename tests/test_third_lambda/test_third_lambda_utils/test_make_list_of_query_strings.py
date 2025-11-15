import pytest
from unittest.mock import Mock, patch

from src.third_lambda.third_lambda_utils.make_list_of_query_strings import make_list_of_query_strings


# Setup:
@pytest.fixture(scope="function")
def setup():
    rows =         [
          (1,  "xxx",   75.50,   None, True ),
          (2,  "yyy",   82.00,   None, False ),
          (3,  "zzz",   69.75,   None, True )
                   ]

    fv1 = ['1', '"xxx"', '75.5', '"NULL"', '"TRUE"']
    fv2 = ['2', '"yyy"', '82.0', '"NULL"', '"FALSE"']
    fv3 = ['3', '"zzz"', '69.75', '"NULL"', '"TRUE"']

    vals_1 = '1, "xxx", 75.5, "NULL", "TRUE"'
    vals_2 = '2, "yyy", 82.0, "NULL", "FALSE"'
    vals_3 = '3, "zzz", 69.75, "NULL", "TRUE"'

    table_name = 'mock_table'

    column_str = 'some_id, col_str, col_float, col_none, col_bool'

    qs_1 = f'INSERT INTO {table_name} ({column_str}) VALUES ({vals_1});'
    qs_2 = f'INSERT INTO {table_name} ({column_str}) VALUES ({vals_2});'
    qs_3 = f'INSERT INTO {table_name} ({column_str}) VALUES ({vals_3});'

    yield rows, fv1, fv2, fv3, qs_1, qs_2, qs_3, table_name, column_str





def test_returns_a_list(setup):
    # Arrange:
    rows, fv1, fv2, fv3, qs_1, qs_2, qs_3, table_name, column_str = setup
    # ensure test can fail:
    expected_fail = 'list'
    expected = list
    

    # Act:
    response = make_list_of_query_strings(rows, table_name, column_str)
    result = type(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail 
    assert result == expected 




# @pytest.mark.skip
def test_calls_function_make_list_of_formatted_row_values_correctly(setup):
    """
    Tests that function make_list_of_query_strings()
    calls function 
    make_list_of_formatted_row_values(row)
    the correct number of times and with the correct 
    args.
    """
    # Arrange:
    rows, fv1, fv2, fv3, qs_1, qs_2, qs_3, table_name, column_str = setup
    # ensure test can fail:

    with patch("src.third_lambda.third_lambda_utils.make_list_of_query_strings.make_list_of_formatted_row_values") as mock_mlofrv:
        mock_mlofrv.side_effect = lambda x: 'x'
        
    
        result = make_list_of_query_strings(rows, table_name, column_str)

        # ensure test can fail:
        # assert mock_mlofrv.call_count == 1
        assert mock_mlofrv.call_count == len(rows)

        # ensure test can fail:
        # mock_mlofrv.assert_any_call(('xxx'))
        mock_mlofrv.assert_any_call((1,  "xxx",   75.50,   None, True ))
        mock_mlofrv.assert_any_call((2,  "yyy",   82.00,   None, False ))
        mock_mlofrv.assert_any_call((3,  "zzz",   69.75,   None, True ))



# @pytest.mark.skip
def test_calls_function_make_query_string_for_one_row_correctly(setup):
    """
    Tests that function make_list_of_query_strings()
    calls function 
    make_query_string_for_one_row(
    formatted_values, 
    table_name, 
    column_str)
    the correct number of times and with the correct 
    args.
    """
    # Arrange:
    rows, fv1, fv2, fv3, qs_1, qs_2, qs_3, table_name, column_str = setup
    # ensure test can fail:

    with patch("src.third_lambda.third_lambda_utils.make_list_of_query_strings.make_query_string_for_one_row") as mock_mqsfor:
        
        result = make_list_of_query_strings(rows, table_name, column_str)

        # print(f'IN test function. This is what mock_mqsfor rxes as args >>> {mock_mqsfor.call_args_list}')
        # Ensure the test can fail:
        # mock_mqsfor.assert_any_call(fv1)
        mock_mqsfor.assert_any_call(fv1,  table_name, column_str)
        mock_mqsfor.assert_any_call(fv2,  table_name, column_str)
        mock_mqsfor.assert_any_call(fv3,  table_name, column_str)






def test_returns_correct_strings(setup):
    # Arrange:
    rows, fv1, fv2, fv3, qs_1, qs_2, qs_3, table_name, column_str = setup
    # ensure test can fail:

    result = make_list_of_query_strings(rows, table_name, column_str)

    # ensure test can fail:
    # assert result == ["", "", ""]
    assert result == [qs_1, qs_2, qs_3]



