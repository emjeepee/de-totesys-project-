



# This function must create a list of dicts from     
# the following two lists:
# 1) 
# ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated']

# 2) 
    # [ [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), datetime.datetime(2025, 6, 4, 8, 58, 10, 6000)],
    # [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), datetime.datetime(2025, 6, 4, 9, 26, 9, 972000)],
    # [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), datetime.datetime(2025, 6, 4, 9, 29, 10, 166000)], etc  ]
 
# 3) List to create:
#   [ { 'transaction_id' :  20496,   'transaction_type' :  'SALE', 'sales_order_id': 14504, etc},
#      { 'transaction_id' :  20497,   'transaction_type' :  'SALE', 'sales_order_id': 14505, etc},  
#       etc
# 
#    ]


def make_row_dicts(col_names: list, row_values: list):
    """
    This function 
    
    """
    list_to_return = []
    num_of_rows = len(row_values)
    num_of_cols = len(col_names)

    for i in range(num_of_rows): 
        list_to_return.append(dict([(col_names[j], row_values[i][j]) for j in range(num_of_cols)]))

    return list_to_return
