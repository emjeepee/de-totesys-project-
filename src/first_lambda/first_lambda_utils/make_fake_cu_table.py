from datetime import datetime


def make_fake_cu_table():
    """
    This function:
        makes a fake currency
        table in the form of a
        list of dictionaries.

    Args:
        None

    Returns:
        A list of dictionaries
        that represents the
        currency table from the
        totesys database.

    """

    base_day = datetime(2025, 11, 13)
    # td_1_day = timedelta(days=1)

    curr_table = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": base_day,
            "last_updated": base_day,
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": base_day,
            "last_updated": base_day,
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": base_day,
            "last_updated": base_day,
        },
    ]

    return curr_table
