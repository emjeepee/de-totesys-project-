from datetime import datetime
from decimal import Decimal


def change_vals_to_strings(ky, val, dct):
    """
    This function:
        Changes the value of a
        dicionary's keys such
        that if the value is a
        datetime.datetime object
        this function changes it
        to iso format string and
        if the value is a
        Decimal object this
        function changes it to
        a string.

    Args:
        ky: a dictionary key

        val: a dcitionary key's
        value, could be anything.

        dct: a dictionary that
        has key ky, whose value
        is val.

    Returns:
        None

    """

    if isinstance(val, datetime):
        dct[ky] = val.isoformat()
    if isinstance(val, Decimal):
        dct[ky] = str(val)
