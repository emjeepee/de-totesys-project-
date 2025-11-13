import pytest

from src.third_lambda.third_lambda_utils.format_value import format_value



def test_returns_correct_string_value_for_None():
    # Arrange:
    value = None
    expected = 'NULL'
    expected_fail = 'artichoke'

    # Act:
    result = format_value(value)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected



def test_returns_correct_string_value_for_string_with_apostrophe():
    # Arrange:
    value = "O'Mally"
    expected = "'O''Mally'"
    expected_fail = 'turnip'

    # Act:
    result = format_value(value)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected


def test_returns_correct_string_value_for_string():
    # Arrange:
    value = "cheese&ham sandwich"
    expected = "'cheese&ham sandwich'"
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
    expected = 'TRUE'
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
    expected = 'FALSE'
    expected_fail = 'turnip'

    # Act:
    result = format_value(value)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected
