

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