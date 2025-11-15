




def format_value(value):
    """
    This function:
        Converts a Python value 
        (that comes from a table 
        row) into a string. 
        Another function will 
        employ the returned value 
        in an SQL query.
        
        changes:
          None to 'NULL',
          string xyz to 'xyz',
          string O'Rourke to 'O''Rourke',
          Boolean True to 'TRUE',
          Boolean False to 'FALSE',  
        
    Args:
        value: the Python value 
        to convert.

    Returns:
        a string that another
        function will use as part 
        of an SQL query.
    
    """
    
    if value is None:
        return '"NULL"'
    elif isinstance(value, str):
        # escape single quotes 
        # by doubling them (as SQL 
        # requires), then put the 
        # resulting value between 
        # double quotes (so that
        # 'O'Rourke' becomes 
        # "O''Rourke"):
        return f'"{value.replace("'", "''")}"'
    elif isinstance(value, bool):
        return '"TRUE"' if value else '"FALSE"'
    else:
        return str(value)