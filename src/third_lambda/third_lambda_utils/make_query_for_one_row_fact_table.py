from src.third_lambda.third_lambda_utils.make_parts_of_a_query_string import make_parts_of_a_query_string


def make_query_for_one_row_fact_table(table_name: str, cols: list, vals: list):
    """
    This function:
        1) makes an SQL query string for 
            one row of the fact table.
        2) gets called by function
            make_row_query_for_correct_table() 
    
    Args:
        1) table_name: a string for the name  
            of a table. Will always be 
            'sales_order'.
        2) cols: a list of column names, eg
            ['www', 'xxx', 'yyy', 'zzz']
        3) vals: a list of values, all strings,
            eg [13, '1', 'NULL', 'sausages']
    
    Returns:
        An SQL query string for one row of the 
         fact table.       
    
    """
    # convert ['design_id', 'xxx', 'yyy']     ->  '(www, xxx,   yyy)' 
    # and     [13,          'NULL', 'turnip'] ->  '(13,  NULL, 'turnip')' :
    cols_vals = make_parts_of_a_query_string(cols, vals)

    # Make a string like this:
    # INSERT INTO sales_order (xxx, yyy, zzz)
    # VALUES (1, NULL, 'turnip');

    sql_query_str = f"INSERT INTO {table_name} {cols_vals[0]} VALUES {cols_vals[1]};"

    return sql_query_str  











    # cols_str = vals_str = ''

    # for i in range(len(cols)):
    #     cols_str += f'{cols[i]}, '
    #     if type(vals[i]) is int: 
    #         vals_str += f"{str(vals[i])}, "            
    #     if type(vals[i]) is str: 
    #         if vals[i] == 'NULL': # get rid of inverted commas:
    #             vals_str += f"{vals[i]}, "
    #         else: # if '1' or 'turnip'
    #             vals_str += f"'{vals[i]}', "                    
            

    # # get rid of ', ' and add 
    # # round brackets: 
    # cols_str = '(' + cols_str[:-2] + ')'
    # vals_str = '(' + vals_str[:-2] + ')'






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
