







def make_SQL_query(table_name:str, cols_str: str, vals_str:str, cols):
    """
    This function:
        Makes an SQL query string
        for one row of a dimension 
        table or of the fact table. 

    Args:
        table_name: the name of the 
         table, eg 'dim_design'

        cols_str: a string made up of 
         the columns of the table, eg
         'design_id, xxx, yyy'

        vals_str: a string made up of 
         the values of one row of the 
         table, eg 
         "13, NULL, 'cabbage'"

        cols: a list of strings, each 
         string being column names of 
         the table, eg
         ['design_id', 'xxx', 'yyy']

    Returns:
        query: the SQL query string.
    
    """

    # If the table is the 
    # facts table:
    if table_name == "facts_sales_order":
            query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str});"
    
    
    # If the table is a 
    # dimensions table:
    else:
            pk = cols[0]
            update_pairs = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)
            query = (
                f"INSERT INTO {table_name} ({cols_str}) VALUES ({vals_str}) "
                f"ON CONFLICT ({pk}) DO UPDATE SET {update_pairs};"
                    )

    return query
