
from unittest.mock import patch 
from src.first_lambda.first_lambda_utils.convert_cell_values_main import convert_cell_values_main

from datetime import datetime
from decimal import Decimal 

import json



def test_calls_aux_function_when_invalid_json_passed_in():
    # Arrange
    not_json_string = 'this is not a json string'

    dt = datetime(2025, 8, 12, 12, 11, 10, 73000)
    
    dpork = Decimal(str(3.1415))

    int_val = 42

    none_val = None
    
    # Act & Assert
    with patch('src.first_lambda.first_lambda_utils.convert_cell_values_main.convert_cell_values_aux') as mock_aux:
        convert_cell_values_main(not_json_string)
        mock_aux.assert_called_once_with(not_json_string)

    with patch('src.first_lambda.first_lambda_utils.convert_cell_values_main.convert_cell_values_aux') as mock_aux:
        convert_cell_values_main(dt)
        mock_aux.assert_called_once_with(dt)

    with patch('src.first_lambda.first_lambda_utils.convert_cell_values_main.convert_cell_values_aux') as mock_aux:
        convert_cell_values_main(dpork)
        mock_aux.assert_called_once_with(dpork)

    with patch('src.first_lambda.first_lambda_utils.convert_cell_values_main.convert_cell_values_aux') as mock_aux:
        convert_cell_values_main(int_val)
        mock_aux.assert_called_once_with(int_val)

    with patch('src.first_lambda.first_lambda_utils.convert_cell_values_main.convert_cell_values_aux') as mock_aux:
        convert_cell_values_main(none_val)
        mock_aux.assert_called_once_with(none_val)



def test_returns_correct_string_when_valid_json_passed_in():
    # Arrange
    not_json_string = 'this is a json string'
    json_string = json.dumps(not_json_string)
    expected = not_json_string

    # Act 
    result = convert_cell_values_main(json_string)
    
    # Assert
    assert result == expected
