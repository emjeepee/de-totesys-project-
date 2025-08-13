import pytest
import datetime
import decimal
import json

from src.first_lambda.first_lambda_utils.convert_values import convert_values





def test_convert_values_returns_similar_list_to_the_one_passed_in():
    
    # Arrange:
    updated_rows =     [ 
        [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), decimal.Decimal(3.1415)],
        [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), decimal.Decimal(3.1416)],
        [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), decimal.Decimal(3.1417)] 
                        ]

    expected1 = len(updated_rows)
    expected2 = len(updated_rows[0])

    # Act:
    output = convert_values(updated_rows)
    result1 = len(output)
    result2 = len(output[0])
        

    # Assert
    assert result1 == expected1
    assert result2 == expected2




# @pytest.mark.skip
def test_convert_values_correctly_converts_values():

    # Arrange:
    updated_rows =     [ 
        [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), decimal.Decimal(3.1415), json.dumps('json_1')],
        [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), decimal.Decimal(3.1416), json.dumps('json_2')],
        [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), decimal.Decimal(3.1417), json.dumps('json_3')] 
                        ]
    
    converted_updated_rows =   [ 
        [20496, 'SALE', 14504, None, '2025-06-04T08:58:10.006000', 3.1415, 'json_1'],
        [20497, 'SALE', 14505, None, '2025-06-04T09:26:09.972000', 3.1416, 'json_2'],
        [20498, 'SALE', 14506, None, '2025-06-04T09:29:10.166000', 3.1417, 'json_3'] 
                        ]

    expected_1 = converted_updated_rows

    # Act:
    result_1 = convert_values(updated_rows)
            

    # Assert
    assert result_1 == expected_1
    
