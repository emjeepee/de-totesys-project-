




def make_vals_for_SQL_query(row):
    """
    This function:
        Creates a string made up
        of the cell values of one 
        row of a dimension table 
        or the fact table that is
        held in a DuckDB 
        dataframe. The string 
        will form part of an SQL 
        query string.  
    
    Args:
        row: a list that represents 
        a row from a dimension 
        table or the facts table,
        eg [13, None, 'cabbage'].

    Returns:
        A string made up of the 
        cell values of one row of 
        a dimension table or the 
        fact table.
    """

    # Make a comma-separated string 
    # of values to insert into the 
    # table. If a value in the row 
    # is a number or NULL, TRUE or
    # FALSE leave the value as is.
    # Otherwise (eg if it is text)
    # wrap it in single quotes:
    vals_str = ", ".join(
           str(v) if isinstance(v, (int, float)) or v in (None, "NULL", "TRUE", "FALSE")
           else f"'{v}'"
           for v in row
                            )   
    
    return vals_str