import pytest

from datetime import datetime
from decimal import Decimal

from src.second_lambda.second_lambda_utils.transform_to_star_schema_fact_table import transform_to_star_schema_fact_table


@pytest.fixture
def general_setup():


        mock_so_table = [
             {'sales_order_id': 15647, 'created_at': datetime(2025, 8, 13, 9, 47, 9, 901000), 'last_updated': datetime(2025, 8, 13, 9, 47, 9, 901000),  'design_id': 648,  'staff_id': 19,  'counterparty_id': 14, 'units_sold': 36692, 'unit_price': Decimal('2.40'), 'currency_id': 2, 'agreed_delivery_date': '2025-08-20', 'agreed_payment_date': '2025-08-16', 'agreed_delivery_location_id': 11},
             {'sales_order_id': 15648, 'created_at': datetime(2025, 9, 13, 9, 47, 9, 901000), 'last_updated': datetime(2025, 9, 13, 9, 47, 9, 901000),  'design_id': 649,  'staff_id': 19,  'counterparty_id': 12, 'units_sold': 36692, 'unit_price': Decimal('2.40'), 'currency_id': 2, 'agreed_delivery_date': '2025-09-20', 'agreed_payment_date': '2025-09-16', 'agreed_delivery_location_id': 12},
                        ]



        yield mock_so_table


# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    () = general_setup

    expected = list

    # Act
    # transform_to_star_schema_fact_table(table_data)
    # result = None
    

    # Assert
    assert result = expected













# @pytest.mark.skip
def test_xxxx(general_setup):
    # Arrange
    () = general_setup

    

    # Act
    
    
    # result = None
    

    # Assert
    assert True




# @pytest.mark.skip
def test_xxxx(general_setup):
    # Arrange
    () = general_setup

    

    # Act
    
    
    # result = None
    

    # Assert
    assert True




