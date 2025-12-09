import pytest

from unittest.mock import Mock, patch, ANY

from src.third_lambda.third_lambda_utils.get_columns_and_rows import get_columns_and_rows


def test_get_columns_and_rows_normal_case():
    # Arrange
    mock_result = Mock()

    mock_result.description = [
        ("id", None, None, None, None, None, None),
        ("name", None, None, None, None, None, None),
        ("price", None, None, None, None, None, None),
                              ]

    mock_rows = [
        (1, "apple", 1.25),
        (2, "banana", 0.75),
                ]
    
    mock_result.fetchall.return_value = mock_rows

    # Act
    column_str, rows = get_columns_and_rows(mock_result)

    # Assert
    assert column_str == '"id", "name", "price"'
    assert rows == mock_rows



def test_fetchall_called_once():
    mock_result = Mock()
    mock_result.description = [("a", None, None, None, None, None, None)]
    mock_result.fetchall.return_value = []

    get_columns_and_rows(mock_result)

    mock_result.fetchall.assert_called_once()