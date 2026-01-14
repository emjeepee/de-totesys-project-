
def make_row_dicts(col_names: list, row_values: list):
    """
    This function:
    creates a list of dictionaries
    that represents a table. Each
    dictionary represents a row
    and its key-value pairs
    represent columnvalue-fieldvalue
    pairs.



    Args:
        1) col_names: a list of
        column names that looks
        like this:
    ['transaction_id',
    'transaction_type',
    'sales_order_id',
    'purchase_order_id',
    'created_at',
    'last_updated']

        2) row_values: a list of
        lists that looks like this:
    [
    [
    20496,
    'SALE',
    14504,
    None,
    datetime.datetime(2025, 6, 4, 8, 58, 10, 6000),
    datetime.datetime(2025, 6, 4, 8, 58, 10, 6000)
    ],

    [
    20497,
    'SALE',
    14505,
    None,
    datetime.datetime(2025, 6, 4, 9, 26, 9, 972000),
    datetime.datetime(2025, 6, 4, 9, 26, 9, 972000)
    ],

    [
    20498,
    'SALE',
    14506,
    None,
    datetime.datetime(2025, 6, 4, 9, 29, 10, 166000),
    datetime.datetime(2025, 6, 4, 9, 29, 10, 166000)
    ],
    etc
    ]
    Where each member list
    contains the field values
    of one row.

    Returns:
    a list of dictionaries that
    looks like the following and
    represents a table:
    [
        {
        'transaction_id' :  20496,
        'transaction_type' :  'SALE',
        'sales_order_id': 14504,
        etc
        },

        {
         'transaction_id' :  20497,
          'transaction_type' :  'SALE',
            'sales_order_id': 14505,
            etc
        },

        etc
    ]

    """


    table = [
             dict(zip(col_names, vals_of_a_row)) 
             for vals_of_a_row in row_values 
            ]
    
    return table







# OLD CODE:
    # list_to_return = []
    # num_rows = len(row_values) # a list of lists
    # num_cols = len(col_names) # a list

    # for i in range(num_rows):
    #     list_to_return.append(
    #         dict([(col_names[j], row_values[i][j]) for j in range(num_cols)])
    #                          )

    # return list_to_return


        