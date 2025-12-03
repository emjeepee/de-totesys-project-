import pytest

from src.third_lambda.third_lambda_utils.format_value import format_value



def test_returns_correct_string_value_for_None():
    # Arrange:
    value = None
    expected = '"NULL"'
    expected_fail = 'turnip'

    # Act:
    result = format_value(value)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected



def test_returns_correct_string_value_for_strings():
    # Arrange:
    # format_value() must create 
    # literal "O''Mally" (represented in python as '"O\'\'Mally"')
    value_1 = "O'Mally"
    expected_1 = '"O\'\'Mally"'
    value_2 = "David Brent"
    expected_2 = '"David Brent"'

    expected_fail = 'turnip'

    # Act:
    result_1 = format_value(value_1)
    result_2 = format_value(value_2)

    # Assert:
    # ensure test can fail:
    # assert result_1 == expected_fail
    assert result_1 == expected_1
    # assert result_2 == expected_fail
    assert result_2 == expected_2


def test_returns_correct_string_value_for_string():
    # Arrange:
    value = 'cheese&ham sandwich'
    expected = '"cheese&ham sandwich"'
    expected_fail = 'turnip'

    # Act:
    result = format_value(value)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected



def test_returns_correct_string_value_for_bool_True():
    # Arrange:
    value = True
    expected = '"TRUE"'
    expected_fail = 'turnip'

    # Act:
    result = format_value(value)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected




# @pytest.mark.skip    
def test_returns_correct_string_value_for_bool_False():
    # Arrange:
    value = False
    expected = '"FALSE"'
    expected_fail = 'turnip'

    # Act:
    result = format_value(value)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected
