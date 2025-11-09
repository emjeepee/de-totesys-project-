


def preprocess_buffer_2(con, result):
    """
    This function:
        creates a list of dictionaries, 
        each dictionary representing 
        a row of a dimension table or
        fact table.
        cols is a list of strings, 
        each string being the name of
        a column, eg
        ['design_id', 'xxx', 'yyy'].
        cols_str is a string of 
        comma-separated column names,
        eg 'design_id, xxx, yyy' 

    Args:
        1) con: a DuckDB connection 
        object in RAM.

        2) result: all rows of a table 
        read by DuckDB (from a Parquet
        file in a BytesIO buffer).
    
    Returns:
        list [row_list, cols, cols_str],
        where:
            row_list is a list of 
            dictionaries, each dictionary
            representing a row of a 
            dimension table or fact table.
            cols is a list of string, 
            each string being the name of
            a column.
            cols_str is a string of 
            comma-separated column names.  
    """



    # Get column names:
    cols = [desc[0] for desc in con.description]  

    # Convert query result into 
    # a list of dicts:
    row_list = [dict(zip(cols, row)) for row in result]

    # Make comma-separated 
    # string of column names:
    cols_str = ", ".join(cols)

    return [row_list, cols, cols_str]
