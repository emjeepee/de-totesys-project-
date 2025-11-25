from datetime import datetime
from decimal import Decimal


from src.first_lambda.first_lambda_utils.change_vals_to_strings import change_vals_to_strings

# Decimal('2.47')
# datetime(2025, 11, 13)

def test_changes_datetime_to_iso_string():
    # arrange:
    ky = "col_1"
    val = datetime(2025, 11, 13)
    dct = {ky: val}
    expected_fail = "artechoke"
    expected = "2025-11-13T00:00:00"

    # act:
    change_vals_to_strings(ky, val, dct)

    # assert:
    # ensure test can fail:
    # assert dct[ky] == expected_fail
    assert dct[ky] == expected


def test_changes_Decimal_to_string():
    # arrange:
    ky = "col_1"
    val = Decimal('2.47')
    dct = {ky: val}
    expected_fail = "artechoke"
    expected = "2.47"

    # act:
    change_vals_to_strings(ky, val, dct)

    # assert:
    # ensure test can fail:
    # assert dct[ky] == expected_fail
    assert dct[ky] == expected    



def test_leaves_some_values_unchanged():
    # arrange:
    ky_0 = "col_1"
    val_0 = 5
    ky_1 = "col_2"
    val_1 = "kebab"
    dct = {ky_0: val_0, ky_1: val_1}
    expected_fail = "artechoke"
    expected_0 = 5
    expected_1 = "kebab"

    # act:
    change_vals_to_strings(ky_0, val_0, dct)
    change_vals_to_strings(ky_1, val_1, dct)

    # assert:
    # ensure test can fail:
    # assert dct[ky_0] == expected_fail
    # assert dct[ky_1] == expected_fail
    assert dct[ky_0] == expected_0
    assert dct[ky_1] == expected_1    
