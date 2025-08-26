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
    dec_pork = Decimal(str(pork))

    result = convert_cell_values_aux(dec_pork)

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_returns_passed_in_value_unchanged():
    # Arrange
    val_1 = 'stringy'
    val_2 = 42

    expected_1 = val_1
    expected_2 = val_2

    # Act
    result_1 = convert_cell_values_aux(val_1)
    result_2 = convert_cell_values_aux(val_2)

    # Assert
    assert result_1 == expected_1
    assert result_2 == expected_2




# @pytest.mark.skip
def test_changes_None_to_no_data():
    # Arrange
    expected_1 = 'no data'

    # Act
    result_1 = convert_cell_values_aux(None)

    # Assert
    assert result_1 == expected_1



# @pytest.mark.skip
def test_changes_runs_of_spaces_or_empty_string_to_no_data():
    # Arrange
    d_1 =''
    d_2 =' '
    d_3 ='  '
    d_4 ='   '
    
    expected = 'no data'

    # Act
    result_1 = convert_cell_values_aux(d_1)
    result_2 = convert_cell_values_aux(d_2)
    result_3 = convert_cell_values_aux(d_3)
    result_4 = convert_cell_values_aux(d_4)

    # Assert
    assert result_1 == expected
    assert result_2 == expected
    assert result_3 == expected
    assert result_4 == expected


# @pytest.mark.skip
def test_changes_Booleans_to_all_caps_string_versions():
    # Arrange
    expected_1 = 'TRUE'
    expected_2 = 'FALSE'

    # Act
    result_1 = convert_cell_values_aux(True)
    result_2 = convert_cell_values_aux(False)

    # Assert
    assert result_1 == expected_1
    assert result_2 == expected_2

