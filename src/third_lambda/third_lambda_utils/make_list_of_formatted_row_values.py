from .format_value import format_value



def make_list_of_formatted_row_values(row):
    """
    This function:
       looks at one row of a 
       table held in a DuckDB 
       database and formats 
       its values so that 
       another function can 
       put those values in an 
       SQL INSERT statement.

    Args:
        row: a row of a table 
        taken from a DuckDB 
        database. The row is
        in the form of a tuple, 
        like this:
        (1,  'xxx',   75.50,   True,   None )


    Returns:
        a list of values of one 
        row of a table held in a
        DuckDB database. The 
        values are in the  
        formats that will allow 
        their use in an SQL 
        INSERT statement.
    
    """

    # formatted_values will hold 
    # the formatted values for 
    # the row:
    formatted_values = []
    # get each value of the row:
    for value in row:  # 5 or "xyx" or 75.5 or None or True
        # format each value to 
        # allow it to be part of 
        # an SQL query string:
        frmttd_val = format_value(value)
        formatted_values.append(frmttd_val) # ['5', '"xyz"', '75.5', '"TRUE"', '"NULL"']    

    return formatted_values