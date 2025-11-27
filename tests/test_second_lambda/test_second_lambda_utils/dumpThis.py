import pytest

from decimal import Decimal
from datetime import datetime 

from src.second_lambda.second_lambda_utils.transform_to_star_schema_fact_table import transform_to_star_schema_fact_table



@pytest.fixture
def setup():

    so_table = [
             {
            'sales_order_id': 15647, 
            'created_at': datetime(2025, 8, 13, 9, 47, 9, 901000), 
            'last_updated': datetime(2025, 8, 13, 9, 47, 9, 901000),  
            'design_id': 64,  
            'staff_id': 1,  
            'counterparty_id': 14, 
            'units_sold': 392, 
            'unit_price': Decimal('2.40'), 
            'currency_id': 2, 
            'agreed_delivery_date':   '2025-08-20', 
            'agreed_payment_date': '2025-08-16', 
            'agreed_delivery_location_id': 11
            },

            {
            'sales_order_id': 15648, 
            'created_at': datetime(2025, 8, 13, 9, 47, 9, 901000), 
            'last_updated': datetime(2025, 8, 13, 9, 47, 9, 901000),  
            'design_id': 48,  
            'staff_id': 2,  
            'counterparty_id': 15, 
            'units_sold': 662, 
            'unit_price': Decimal('2.40'), 
            'currency_id': 3, 
            'agreed_delivery_date':   '2025-08-20', 
            'agreed_payment_date': '2025-08-16', 
            'agreed_delivery_location_id': 12
            },

             {
            'sales_order_id': 15649, 
            'created_at': datetime(2025, 8, 13, 9, 47, 9, 901000), 
            'last_updated': datetime(2025, 8, 13, 9, 47, 9, 901000),  
            'design_id': 648,  
            'staff_id': 3,  
            'counterparty_id': 16, 
            'units_sold': 366, 
            'unit_price': Decimal('2.40'), 
            'currency_id': 4, 
            'agreed_delivery_date':   '2025-08-20', 
            'agreed_payment_date': '2025-08-16', 
            'agreed_delivery_location_id': 13
            },
                        ]




    yield so_table    
    






def test_returns_list(setup):
    # Arrange: 
    so_table = setup
    expected_fail = str
    expected      = list 

    # Act:
    response = transform_to_star_schema_fact_table(so_table)
    result = type(response)

    #Assert:  
    # ensure test can fail:
    assert result == expected_fail
    assert result == expected

    