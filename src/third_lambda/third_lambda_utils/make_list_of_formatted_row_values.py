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
        a list that contains 
        field values of one
        row of a table held in 
        a DuckDB database. The
        values are in formats 
        that will allow their 
        use in an SQL INSERT 
        statement.

    """

    # value (below) is 5 or "xyx" or 75.5 or None or True
    # ['5', '"xyz"', '75.5', '"NULL"', '"TRUE"']
    # formatted_values is ['5', '"xyz"', '75.5', '"NULL"', '"TRUE"']
    formatted_values = [ format_value(value)  for value in row ]

    return formatted_values