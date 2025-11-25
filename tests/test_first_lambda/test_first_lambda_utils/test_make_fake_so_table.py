import pytest

from datetime import datetime, date, time, timedelta
from decimal import Decimal

from src.first_lambda.first_lambda_utils.make_fake_so_table import make_fake_so_table



def test_returns_a_list():
    # Arrange:
    expected_fail = str
    expected = list

    # Act
    response = make_fake_so_table()
    result = type(response)

    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected




# @pytest.mark.skip
def test_returns_list_of_fifty_dicts():
    # Arrange:
    expected_fail = 42
    expected = 50

    # Act:
    response = make_fake_so_table()
    result = len(response)

    # Assert:
    # ensure test can fail:
    # assert result == expected_fail
    assert result == expected 



# @pytest.mark.skip
def test_returns_correct_list():
    # Arrange:
    base_day = datetime(2025, 11, 13, 15, 17, 8) 
    td_1_day = timedelta(days=1)
    expected_fail = "expected_fail"
    expected_so_id = 38
    expected_ca = base_day + (38 * td_1_day)
    expected_lu = base_day + (38 * td_1_day) + td_1_day 
    expected_add = (base_day + (38 * td_1_day) + ((5 * td_1_day))).strftime('%Y-%m-%d')
    expected_apd = (base_day + (38 * td_1_day) + ((3 * td_1_day))).strftime('%Y-%m-%d')


    # expected = {
    #         'sales_order_id':   38,
    #         'created_at':		base_day + (38 * td_1_day),
    #         'last_updated':		base_day + (38 * td_1_day) \
    #                             + td_1_day if 38 % 2 == 0 else  \
    #                             base_day + (i * td_1_day),
    #         'design_id':		random.randint(1, 10),  # inclusive
    #         'staff_id': 		random.randint(1, 10),
    #         'counterparty_id':	random.randint(1, 10),
    #         'units_sold': 		random.randint(1, 100),
    #         'unit_price': 		Decimal(str(random.randint(100, 999) / 100)),
    #         'currency_id':		random.randint(1, 3),
    #         'agreed_delivery_date':	(base_day + (38 * td_1_day) + ((5 * td_1_day))).strftime('%Y-%m-%d'),
    #         'agreed_payment_date':  (base_day + (38 * td_1_day) + ((3 * td_1_day))).strftime('%Y-%m-%d'),
    #         'agreed_delivery_location_id':	random.randint(1, 10),
    #             }

    # Act
    response = make_fake_so_table()
    result = response[37]
    result_so_id = result["sales_order_id"]
    result_ca = result["created_at"]
    result_lu = result["last_updated"]
    result_add = result['agreed_delivery_date']
    result_apd = result['agreed_payment_date']

    result_did = result['design_id']
    result_sid = result['staff_id']
    result_cid = result['counterparty_id']
    result_us = result['units_sold']
    result_up_type = type(result['unit_price'])
    result_curid = result['currency_id']
    result_adlid = result['agreed_delivery_location_id']


    # Assert
    # ensure test can fail:
    # assert result == expected_fail
    assert result_so_id == expected_so_id
    assert result_ca == expected_ca
    assert result_lu == expected_lu
    assert result_add == expected_add
    assert result_apd == expected_apd
    assert result_did > 0 and result_did < 11 
    assert result_sid > 0 and result_sid < 11 
    assert result_cid > 0 and result_cid < 11 
    assert result_us > 0 and result_us < 101 
    assert result_up_type == Decimal
    assert result_curid > 0 and result_curid < 4 
    assert result_adlid > 0 and result_adlid < 11 


