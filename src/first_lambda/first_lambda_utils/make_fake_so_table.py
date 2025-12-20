import random
from decimal import Decimal

from datetime import datetime, timedelta


def make_fake_so_table():
    """
    This function:
        makes a fale sales_order
        table in the form of a
        list of dictionaries

    Args:
        None

    Returns:
        A list of dictionaries
        that represents the
        sales_order table
        from the totesys database.

    """

    # sales_order table
    # the sales order table from the
    # totesys OLAP database must look
    # like this:
    #     'sales_order_id'	15647
    #     'created_at’		datetime.datetime(2025, 8, 13, 9, 47, 9, 901000)
    #     'last_updated',	datetime.datetime(2025, 8, 13, 9, 47, 9, 901000)
    #     'design_id',		648
    #     'staff_id', 		19
    #     'counterparty_id’		14
    #     'units_sold', 		36692
    #     'unit_price', 		Decimal('2.40')
    #     'currency_id'		2
    #     'agreed_delivery_date',	'2025-08-20'
    #     'agreed_payment_date',  '2025-08-16'
    #     'agreed_delivery_location_id'	11

    # ''.join(chr(random.randint(97, 122)) for _ in range(5)),

    # There will be 50 rows in the
    # sales order table and 10 rows in
    # each of the other tables except
    # the department table and the
    # currency table.

    # In the sales_orders table:
    # value of first 'created_at' must be base_day
    # make consecutive 'created_at' values base_day + 1 day,
    #                       base_day + 2 days, etc
    # make all 'last_updated' values for even ids 'created_at' + 1day
    # make all 'last_updated' values for odd ids 'created_at'
    # 'sales_order_id' values will go from 1 to 50 in order
    # 'agreed_delivery_date':	'2025-08-20'  must be five days after
    #                                                       'created_at'
    # 'agreed_payment_date':  '2025-08-16',  must be three days after
    #                                                       'created_at'
    # 'unit_price' must be random string version of 2-digit float. There must
    # be 10 design_id, one for each price, so will need a lookup table
    # 'unit_price' must have value 1.00 to 9.99 as a string

    base_day = datetime(2025, 11, 13, 15, 17, 8)
    td_1_day = timedelta(days=1)

    so_table = [
        {
            "sales_order_id": i,
            "created_at": base_day + (i * td_1_day),
            "last_updated": (
                base_day + (i * td_1_day) + td_1_day
                if i % 2 == 0
                else base_day + (i * td_1_day)
            ),
            "design_id": random.randint(1, 10),  # inclusive
            "staff_id": random.randint(1, 10),
            "counterparty_id": random.randint(1, 10),
            "units_sold": random.randint(1, 100),
            "unit_price": Decimal(str(random.randint(100, 999) / 100)),
            "currency_id": random.randint(1, 3),
            "agreed_delivery_date": (
                base_day + (i * td_1_day) + ((5 * td_1_day))
            ).strftime("%Y-%m-%d"),
            "agreed_payment_date": (
                base_day + (i * td_1_day) + ((3 * td_1_day))
            ).strftime("%Y-%m-%d"),
            "agreed_delivery_location_id": random.randint(1, 10),
        }
        for i in range(1, 51)  # 1->50
    ]

    return so_table
