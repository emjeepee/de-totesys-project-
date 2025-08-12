

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
# 2) dim_staff 
# has a column  whose value must be an SQL email address
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


# These tables have columns whose values are 
# all either varchars or ints:
# dim_counterparty
# dim_currency
# dim_design
# dim_location
