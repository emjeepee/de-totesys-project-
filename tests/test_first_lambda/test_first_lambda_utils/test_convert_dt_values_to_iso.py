import pytest
import datetime
import decimal

from src.first_lambda.first_lambda_utils.convert_dt_values_to_iso import convert_dt_values_to_iso





def test_convert_dt_values_to_iso_returns_similar_list_to_the_one_passed_in():
    
    # Arrange:
    list_of_lists =     [ 
        [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), decimal.Decimal(3.1415)],
        [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), decimal.Decimal(3.1416)],
        [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), decimal.Decimal(3.1417)] 
                        ]

    expected1 = len(list_of_lists)
    expected2 = len(list_of_lists[0])

    # Act:
    output = convert_dt_values_to_iso(list_of_lists)
    result1 = len(output)
    result2 = len(output[0])
        

    # Assert
    assert result1 == expected1
    assert result2 == expected2




# @pytest.mark.skip
def test_convert_dt_values_to_iso_correctly_converts_values():

    # Arrange:
    list_of_lists =     [ 
        [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), decimal.Decimal(3.1415)],
        [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), decimal.Decimal(3.1416)],
        [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), decimal.Decimal(3.1417)] 
                        ]
    
    expected1 =   [ 
        [20496, 'SALE', 14504, None, '2025-06-04T08:58:10.006000', 3.1415],
        [20497, 'SALE', 14505, None, '2025-06-04T09:26:09.972000', 3.1416],
        [20498, 'SALE', 14506, None, '2025-06-04T09:29:10.166000', 3.1417] 
                        ]


    # Act:
    result1 = convert_dt_values_to_iso(list_of_lists)
            

    # Assert
    assert result1 == expected1
    
