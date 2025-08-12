

def make_SQL_qry_for_one_row(table_name: str, pk_col, cols, vals, row):
    """
    This function:
        makes an SQL query string depending
        on the value of tabel_name.
    Args:
        table_name: a string for the name of 
         a table.
        pk_col: the name of the primary key.
        cols: a string of comma-separated 
         table column names. 
        vals: a string of comma-separated cell
         values for a table row. 
        row: a row from a pandas dataFrame.            
    Returns:
        An SQL query string the exact form of 
         which depends on whether the table is 
         a dimension table or the fact table.       
    
    """

    if table_name == 'sales_order': # if it's the fact table
        sql_query_str = f"INSERT INTO {table_name} ({cols}) VALUES ({vals});"
    else: # if it's a dimensions table
        sql_query_str = (
                f"DELETE FROM {table_name} WHERE {pk_col} = '{row[pk_col]}'; "
                f"INSERT INTO {table_name} ({cols}) VALUES ({vals});"
                            )   
    return sql_query_str        


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
