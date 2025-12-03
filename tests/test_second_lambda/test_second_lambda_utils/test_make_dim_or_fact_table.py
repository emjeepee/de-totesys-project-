import pytest 


from unittest.mock import Mock, patch
from datetime import datetime
from decimal import Decimal


from src.second_lambda.second_lambda_utils.make_dim_or_fact_table import make_dim_or_fact_table






@pytest.fixture
def setup():
        mock_tbl_name_1  = 'staff'
        mock_tbl_name_2  = 'counterparty'
        mock_tbl_name_3  = 'location'
        mock_tbl_1       = 'dept or addr table' 
        mock_tbl_2       = 'dim or fact table' 
        mock_tabl_python = [{}, {}] 
        mock_s3_client   = 'mock_s3_client'
        mock_ing_bucket  = 'mock_ing_bucket'

        cre_at_iso_1 = datetime(2025, 8, 13, 9, 47, 9).isoformat()
        cre_at_iso_2 = datetime(2025, 8, 13, 9, 47, 9).isoformat()
        las_up_iso_1 = datetime(2025, 8, 14, 9, 47, 9).isoformat()
        las_up_iso_2 = datetime(2025, 8, 14, 9, 47, 9).isoformat()


        mock_so_table = [
             {'sales_order_id': 15647, 'created_at': cre_at_iso_1, 'last_updated': las_up_iso_1,  'design_id': 648,  'staff_id': 19,  'counterparty_id': 14, 'units_sold': 62, 'unit_price': Decimal('2.40'), 'currency_id': 2, 'agreed_delivery_date': '2025-08-20', 'agreed_payment_date': '2025-08-16', 'agreed_delivery_location_id': 11},
             {'sales_order_id': 15648, 'created_at': cre_at_iso_2, 'last_updated': las_up_iso_2,  'design_id': 649,  'staff_id': 2,  'counterparty_id': 1, 'units_sold': 36, 'unit_price': Decimal('4.63'), 'currency_id': 1, 'agreed_delivery_date': '2025-09-20', 'agreed_payment_date': '2025-09-16', 'agreed_delivery_location_id': 12},
                        ]

        yield mock_so_table, mock_tbl_name_1, mock_tbl_name_2, mock_tbl_name_3, mock_tbl_1, mock_tbl_2, mock_tabl_python, mock_s3_client, mock_ing_bucket 
        





@pytest.mark.skip
def test_calls_internal_functions_correctly_when_table_name_is_staff(setup):
    # Arrange:
    mock_so_table, mock_tbl_name_1, mock_tbl_name_2, mock_tbl_name_3, mock_tbl_1, mock_tbl_2, mock_tabl_python, mock_s3_client, mock_ing_bucket = setup
    expected = ['staff_dim_table']
    # expected = ''

    # Act:
    with patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.make_staff_or_cp_dim_table') as mock_msocdt, \
        patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.func_lookup_table') as mock_flt: 
        mock_msocdt.return_value = ['staff_dim_table']
        result = make_dim_or_fact_table(mock_tbl_name_1, mock_tabl_python, mock_s3_client, mock_ing_bucket)
        mock_msocdt.assert_called_once_with(mock_tbl_name_1, mock_tabl_python, mock_flt, mock_ing_bucket, 'department', mock_s3_client)
        assert result == expected


@pytest.mark.skip
def test_makes_calls_to_internal_functions_correct_when_table_name_is_counterparty(setup):
    # Arrange:
    mock_so_table, mock_tbl_name_1, mock_tbl_name_2, mock_tbl_name_3, mock_tbl_1, mock_tbl_2, mock_tabl_python, mock_s3_client, mock_ing_bucket = setup
    expected = ['counterparty_dim_table']
    # expected = ''

    # Act:
    with patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.make_staff_or_cp_dim_table') as mock_msocdt, \
        patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.func_lookup_table') as mock_flt: 
        mock_msocdt.return_value = ['counterparty_dim_table']
        result = make_dim_or_fact_table(mock_tbl_name_2, mock_tabl_python, mock_s3_client, mock_ing_bucket)

        # Assert:
        mock_msocdt.assert_called_once_with(mock_tbl_name_2, mock_tabl_python, mock_flt, mock_ing_bucket, 'address', mock_s3_client)
        assert result == expected


# @pytest.mark.skip
def test_makes_calls_to_internal_functions_correct_when_table_name_is_location(setup):
    # Arrange:
    mock_so_table, mock_tbl_name_1, mock_tbl_name_2, mock_tbl_name_3, mock_tbl_1, mock_tbl_2, mock_tabl_python, mock_s3_client, mock_ing_bucket = setup
    expected = ['location_dim_table']
    # expected = ''

    # Act:
    with patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.func_lookup_table') as mock_flt: 
        mock_flt_returned_func = Mock()
        mock_flt_returned_func.return_value = ['location_dim_table']
        mock_flt.return_value = mock_flt_returned_func
        result = make_dim_or_fact_table(mock_tbl_name_3, mock_tabl_python, mock_s3_client, mock_ing_bucket)

        # Assert:
        mock_flt.assert_called_once_with(mock_tbl_name_3)
        assert result == expected


# currency, design, location, sales_order
 



 # @pytest.mark.skip
def test_returns_fact_table_correctly(setup):
    # Arrange:
    mock_so_table, mock_tbl_name_1, mock_tbl_name_2, mock_tbl_name_3, mock_tbl_1, mock_tbl_2, mock_tabl_python, mock_s3_client, mock_ing_bucket = setup

        # cre_at_iso_1 = datetime(2025, 8, 13, 9, 47, 9).isoformat()
        # cre_at_iso_2 = datetime(2025, 8, 13, 9, 47, 9).isoformat()
        # las_up_iso_1 = datetime(2025, 8, 14, 9, 47, 9).isoformat()
        # las_up_iso_2 = datetime(2025, 8, 14, 9, 47, 9).isoformat()
    
        # mock_so_table = [
        #      {'sales_order_id': 15647, 'created_at': cre_at_iso_1, 'last_updated': las_up_iso_1,  'design_id': 648,  'staff_id': 19,  'counterparty_id': 14, 'units_sold': 62, 'unit_price': Decimal('2.40'), 'currency_id': 2, 'agreed_delivery_date': '2025-08-20', 'agreed_payment_date': '2025-08-16', 'agreed_delivery_location_id': 11},
        #      {'sales_order_id': 15648, 'created_at': cre_at_iso_2, 'last_updated': las_up_iso_2,  'design_id': 649,  'staff_id': 2,  'counterparty_id': 1, 'units_sold': 36, 'unit_price': Decimal('4.63'), 'currency_id': 1, 'agreed_delivery_date': '2025-09-20', 'agreed_payment_date': '2025-09-16', 'agreed_delivery_location_id': 12},
        #                 ]

    iso_ca_date_0 = mock_so_table[0].get("created_at")
    dt_created = datetime.fromisoformat(iso_ca_date_0)
    dt_created_time = dt_created.time() # extract time only

    iso_up_date_0 = mock_so_table[0].get("last_updated")
    dt_updated = datetime.fromisoformat(iso_up_date_0)
    dt_updated_time = dt_updated.time() # extract time only


    expected_fail = "boiled cabbage"
    expected_c_time = dt_created_time
    expected_lu_time = dt_updated_time


    
    # expected = [{
    #         "sales_order_id": 15647,
    #         "created_date": dt_created_date,                # in warehouse is date NN 
    #         "created_time": dt_created_time,                # in warehouse is time NN 
    #         "last_updated_date": dt_updated_date,           # in warehouse is date NN 
    #         "last_updated_time": dt_updated_time,           # in warehouse is time NN 
    #         "sales_staff_id": row.get("staff_id"),          # in warehouse is int NN
    #         "counterparty_id": row.get("counterparty_id"),  # in warehouse is int NN
    #         "units_sold": row.get("units_sold"),            # in warehouse is int NN
    #         "unit_price": up_form,                          # in warehouse is numeric(10,2)
    #         "currency_id": row.get("currency_id"),          # in warehouse is int NN
    #         "design_id": row.get("design_id"),              # in warehouse is int NN
    #         "agreed_payment_date": dt_apd,                  # in warehouse is date NN 
    #         "agreed_delivery_date": dt_add,                 # in warehouse is date NN 
    #         "agreed_delivery_location_id": \
    #             row.get("agreed_delivery_location_id"),     # in warehouse is int NN
    #                     }

    #              ]

    # Act:
    # make_dim_or_fact_table(table_name: str, table_python: list, s3_client, ingestion_bucket: str)
    response = make_dim_or_fact_table('sales_order', mock_so_table, mock_s3_client, mock_ing_bucket)
    result_c_time  = response[0]["created_time"]
    result_lu_time = response[0]["last_updated_time"]


    # Assert:
    # ensure test can fail:
    # assert result_c_time == expected_fail
    assert result_c_time == expected_c_time
    assert result_lu_time == expected_lu_time
 