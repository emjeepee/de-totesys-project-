from src.third_lambda.third_lambda_utils.make_parts_of_a_query_string import make_parts_of_a_query_string



def make_query_for_one_row_dim_table(table_name: str, pk_col: str, cols: list, vals: list):
    """
    This function:
        makes an SQL query string for one row 
         of a dimension table. Another 
         function will later make a list of 
         strings like it. Another function 
         again will later make the queries in
         the list to the warehouse.
    
    Args:
        1) table_name: a string for the name of 
            a table.
        2) pk_col: the name of the primary key.
        3) cols: a list of column names, eg
            ['design_id', 'xxx', 'yyy', 'zzz']
        4) vals: a list of values, all strings,
            eg [13, '1', 'NULL', 'sausages']
    
    Returns:
        An SQL query string for one row of a 
         dimension table. Later functions 
         will assemble strings such as this 
         one into a list and use them to 
         query the warehouse.      
    
    """

    # make list [cols_str, vals_str, col_val_pairs]
    # where cols_str is a string of comma-separated 
    # column names, vals_str is a string of 
    # comma-separated row values and col_val_pairs 
    # is a string containing comma-separated 
    # column-value pairs:
    parts_lst = make_parts_of_a_query_string(cols, vals) # [cols_str, vals_str]


    # make the column-value pairs
    # cols looks like this: ['design_id', 'xxx', 'yyy']
    # vals looks like this: [13, 'NULL', 'sausages'].
    # NOTE: for the five dimension tables and the fact 
    # table of this project, table values are never 
    # actually booleans and generally won't be NULL.
    col_val_pairs = ", ".join(
        f"{col} = {val}" if isinstance(val, int) or val in ('NULL', 'TRUE', 'FALSE')
        else f"{col} = '{val}'"
        for col, val in zip(cols, vals)  # NOTE: args for zip() can be any types of iterable
                             ) # NOTE the arg to join() is a
                               # generator expression, which 
                               # returns an iterable, and 
                               # method join() takes an 
                               # iterable as arg. As the 
                               # generator expression is the 
                               # only argument of join() you 
                               # don't need round brackets 
                               # around it.    

    col_val_pairs = col_val_pairs + ';'          


    return f'INSERT INTO {table_name} {parts_lst[0]} VALUES {parts_lst[1]} ON CONFLICT {pk_col} DO UPDATE SET {col_val_pairs}'        






    # col_val_pairs = ''
    # for i in range(len(cols)):
    #     if type(vals[i]) == int:
    #         col_val_pairs += f"{cols[i]} = {str(vals[i])}, "
    #     else:
    #         # if 'turnip' keep the inverted commas, 
    #         # if 'NULL' or "TRUE" or "FALSE" get 
    #         # rid of them:
    #         col_val_pairs += f"{cols[i]} = {vals[i]}, " if vals[i] in ('NULL', 'TRUE', 'FALSE') else f"{cols[i]} = '{vals[i]}', "







    # if type(vals[i]) is int: 
    #         vals_str += f"{str(vals[i])}, "            
    # if type(vals[i]) is str: 
    #     if vals[i] == 'NULL': # get rid of inverted commas:
    #         vals_str += f"{vals[i]}, "
    #     else: # eg if '1' or 'turnip' keep the inverted commas:
    #         vals_str += f"'{vals[i]}', "






# IMPORTANT!!!:



# from datetime import date

# # Get today's date
# created_date = date.today()

# # Convert to string for SQL
# created_date_str = created_date.strftime("%Y-%m-%d")

# print(created_date_str)  # e.g., '2025-08-11'


# NOTE IMPORTANT!!!: 
# 1) dim_date 
# has a date_id column whose value must be an SQL date
# (the rest are ints or varchars)

# So somewhere do this:
# from datetime import date

# date_dimensions_list = [
#     {
#         "date_id": date(2025, 8, 11),
#         "year": 2025,
#         "month": 8,
#         "day": 11
#     },
#     {
#         "date_id": date(2025, 8, 12),
#         "year": 2025,
#         "month": 8,
#         "day": 12
#     }
# ]



# 2) dim_staff 
# has a column  whose value must be an SQL email_address
# NOTE: this must be a mistake!!! apparently postgreSQL dbs
# have no email_address SQL types, so just make it a string!!!
# (the rest are ints or varchars)

# 3) fact_sales_order 
# has a created_date column whose values must be SQL dates
# has a created_time column whose values must be SQL times
# has a last_updated_date column whose values must be SQL dates
# has a last_updated_time column whose values must be SQL times
# has a unit_price column whose values must be SQL numeric(10,2)
# has a agreement_payment_date column whose values must be SQL dates
# has a agreement_delivery_date column whose values must be SQL dates
# (the rest are ints or varchars)

# So somewhere do this for times:
# from datetime import time

# fact_sales_order_list = [
#     {
#         "order_id": 1,
#         "created_time": time(14, 30, 45),  # 14:30:45
#         "amount": 100.50
#     },
#     {
#         "order_id": 2,
#         "created_time": time(9, 15, 0),    # 09:15:00
#         "amount": 250.75
#     }
# ]

# and this for unit_price numeric(10,2):
# from decimal import Decimal

# fact_sales_order_list = [
#     {
#         "order_id": 1,
#         "unit_price": Decimal("19.99"),  # Exactly 19.99, no float errors
#         "quantity": 2
#     },
#     {
#         "order_id": 2,
#         "unit_price": Decimal("2500.00"),  # Exactly 2500.00
#         "quantity": 1
#     }
# ]







# These tables have columns whose values are 
# all either varchars or ints:
# dim_counterparty
# dim_currency
# dim_design
# dim_location





    # if dimension table:
    # sql_query_str = (
    #         f"DELETE FROM {table_name} WHERE {pk_col} = '{row_data[pk_col]}'; "
    #         f"INSERT INTO {table_name} ({cols}) VALUES ({vals});"
    #                     )   
    # Want to build this:
    # INSERT INTO design (aaa, bbb, ccc) 
    # VALUES (1, 2, 3)
    # ON CONFLICT (design_id)
    # DO UPDATE SET aaa = 1, bbb = 2, ccc = 3;
