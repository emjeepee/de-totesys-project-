
import calendar
import pytest

from datetime import timedelta, datetime

from src.second_lambda.second_lambda_utils.make_dim_date_python import make_dim_date_python



@pytest.fixture(scope="function")
def general_setup():
    start_date = datetime(24, 1, 1)
    loop_range = 3

    yield start_date, loop_range



def test_returns_list(general_setup):
    # Arrange
    (start_date, loop_range) = general_setup
    expected = list

    # Act
    response = make_dim_date_python(start_date, loop_range)
    result = dict
    # result = type(response)

    # Assert
    assert result == expected

