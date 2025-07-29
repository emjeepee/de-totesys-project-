from unittest.mock import Mock, patch, ANY
from src.first_lambda.first_lambda_utils.serialise_datetime import serialise_datetime

import datetime
from decimal import Decimal 




def test_serialise_datetime_returns_correct_string():
    # Arrange
    dt_obj = datetime.datetime(1900, 1, 1, 1, 1, 1) # year, month, date, hour, mins, secs
    d_obj = Decimal('3.14159')

    # Act
    result1 = serialise_datetime(dt_obj)
    expected1 = '1900-01-01T01:01:01'

    result2 = serialise_datetime(d_obj)
    expected2 = '3.14159'

    # Assert
    assert result1 == expected1
    assert result2 == expected2

    