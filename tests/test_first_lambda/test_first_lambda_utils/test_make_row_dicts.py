import pytest
from unittest.mock import patch, Mock

from src.first_lambda.first_lambda_utils.make_row_dicts import make_row_dicts


# @pytest.mark.skip(reason="Skipping this test to perform only the previous tests")


col_names = ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated']
row_values = [ 
    [20496, 'SALE', 14504, None, '2025-06-04T08:58:10:6000', '2025-06-04T08:58:10:6000'],
    [20497, 'SALE', 14505, None, '2025-06-04T08:58:10:6000', '2025-06-04T08:58:10:6000'],
    [20498, 'SALE', 14506, None, '2025-06-04T08:58:10:6000', '2025-06-04T08:58:10:6000']  
    ]


def test_make_row_dicts_returns_list_of_dicts():
    # Arrange
    yes_counter = 0
    no_counter = 0
    expected1 = list
    expected2 = len(row_values)

    # Act
    output = make_row_dicts(col_names, row_values)
    result1 = type(output)
    
    for i in range(len(output)):
        if isinstance(output[i], dict):
            yes_counter +=1
        else:
            no_counter +=1                        

    result2 = yes_counter           



    # Assert
    assert result1 == expected1
    assert result2 == expected2
    







# @pytest.mark.skip
def test_make_row_dicts_returns_list_of_dicts_with_correct_keys_in_the_dicts():
    # Arrange
    output = make_row_dicts(col_names, row_values)
    expected1 = 3
    expected2 = 0

    yes_counter = 0
    no_counter = 0

    # Act
    # output is [{'transaction_id': 'xxx', 'transaction_type': 'yyy', etc}, {'transaction_id': 'aaa', 'transaction_type': 'bbb', etc}, etc]
    
    for i in range(len(output)):
        if list(output[i].keys()) == col_names:
            yes_counter += 1
        else:
            no_counter += 1
            # print(f'output[i].keys() is >>> {output[i].keys()}')
    
    result1 = yes_counter
    result2 = no_counter           


    # Assert
    assert result1 == expected1
    assert result2 == expected2
        

def test_make_row_dicts_returns_list_of_dicts_with_correct_values_for_keys_in_the_dicts():
    # Arrange
    output = make_row_dicts(col_names, row_values)
    expected1 = 3


    yes_counter = 0


    # Act
    # output is [{'transaction_id': 'xxx', 'transaction_type': 'yyy', etc}, {'transaction_id': 'aaa', 'transaction_type': 'bbb', etc}, etc]
    
    list_of_keys = col_names
    for i in range(len(output)): # for each dict
        list_of_values = []
        for j in range(len(list_of_keys)):  # for each key
            list_of_values.append(output[i][list_of_keys[j]])
        if list_of_values == row_values[i]:
            yes_counter += 1
    
    result1 = yes_counter


    # Assert
    assert result1 == expected1

