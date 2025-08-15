from src.first_lambda.first_lambda_utils.convert_cell_values_aux import convert_cell_values_aux

from datetime import datetime
from decimal import Decimal 


import pytest




def test_returns_string_version_of_datetime_object():
    # Arrange
    dt = datetime(2025, 8, 12, 12, 11, 10, 73000)
    # The standard ISO 8601 format for Python/parquet/pandas is
    # "2025-08-12T12:11:10.073000"
    expected = "2025-08-12T12:11:10.073000"


    # Act
    result = convert_cell_values_aux(dt)

    # Assert
    
    assert result == expected


# @pytest.mark.skip
def test_returns_float_version_of_Decimal_object():
    # Arrange
    pork = 3.1415
    expected = pork
    dpork = Decimal(str(pork))

    result = convert_cell_values_aux(dpork)

    # Assert
    
    assert result == expected




# @pytest.mark.skip
def test_returns_passed_in_value_unchanged():
    # Arrange
    val_1 = 'stringy'
    val_2 = 42
    val_3 = None

    expected_1 = val_1
    expected_2 = val_2
    expected_3 = val_3


    # Act
    result_1 = convert_cell_values_aux(val_1)
    result_2 = convert_cell_values_aux(val_2)
    result_3 = convert_cell_values_aux(val_3)


    # Assert
    
    assert result_1 == expected_1
    assert result_2 == expected_2
    assert result_3 == expected_3
