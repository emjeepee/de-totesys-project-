import pytest

from datetime import datetime, date, time
from decimal import Decimal

from src.second_lambda.second_lambda_utils.transform_to_star_schema_fact_table import transform_to_star_schema_fact_table


@pytest.fixture
def general_setup():
        keys = [
                "sales_order_id",
                "created_date",
                "created_time",
                "last_updated_date",
                "last_updated_time",
                "sales_staff_id",
                "counterparty_id",
                "units_sold",
                "unit_price",
                "currency_id",
                "design_id",
                "agreed_payment_date",
                "agreed_delivery_date",
                "agreed_delivery_location_id"
                       ]
        
        types =     [
             int,
             date,
             time,
             date,
             time,
             int,
             int,
             int,
             str,
             int,
             int,
             date,
             date,
             int
                    ]


        mock_so_table = [
             {'sales_order_id': 15647, 'created_at': datetime(2025, 8, 13, 9, 47, 9, 901000), 'last_updated': datetime(2025, 8, 13, 9, 47, 9, 901000),  'design_id': 648,  'staff_id': 19,  'counterparty_id': 14, 'units_sold': 36692, 'unit_price': Decimal('2.40'), 'currency_id': 2, 'agreed_delivery_date': '2025-08-20', 'agreed_payment_date': '2025-08-16', 'agreed_delivery_location_id': 11},
             {'sales_order_id': 15648, 'created_at': datetime(2025, 9, 13, 9, 47, 9, 901000), 'last_updated': datetime(2025, 9, 13, 9, 47, 9, 901000),  'design_id': 649,  'staff_id': 19,  'counterparty_id': 12, 'units_sold': 36692, 'unit_price': Decimal('4.63'), 'currency_id': 1, 'agreed_delivery_date': '2025-09-20', 'agreed_payment_date': '2025-09-16', 'agreed_delivery_location_id': 12},
                        ]

        yield keys, types, mock_so_table





# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    ( keys, types, mock_so_table) = general_setup

    expected = list

    # Act
    response = transform_to_star_schema_fact_table(mock_so_table)
    result = type(response)
    # result = None
    

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_fact_table_has_same_number_or_rows_as_sales_order_table(general_setup):
    # Arrange:
    ( keys, types, mock_so_table) = general_setup
    expected = len(mock_so_table)        

    # Act:
    response = transform_to_star_schema_fact_table(mock_so_table) # [{}, {}, {}, etc]
    result = len(response)
    # result = None
    
    # Assert:
    assert result == expected




# @pytest.mark.skip
def test_each_row_of_fact_table_has_correct_keys(general_setup):
    # Arrange:
    ( keys, types, mock_so_table) = general_setup
    is_equal = True

    # Act:
    response = transform_to_star_schema_fact_table(mock_so_table) # [{}, {}, {}, etc]
    for dict in response:
        for i in range(14):
             if not list(dict.keys())[i] == keys[i]: 
                is_equal = not is_equal
                break

    result = is_equal
    # result = False
    
    # Assert:
    assert result 







# @pytest.mark.skip
def test_each_row_of_fact_table_has_values_of_correct_type(general_setup):
    # Arrange
    ( keys, types, mock_so_table) = general_setup
    is_equal = True
   

    # Act
    response = transform_to_star_schema_fact_table(mock_so_table) # [{}, {}, {}, etc]
    for dict in response:
        for i in range(14):
             val = list(dict.values())[i]
             val_type = type(val)
             if not val_type == types[i]: 
                is_equal = not is_equal
                break

    result = is_equal
    # result = False


    # Assert
    assert result




