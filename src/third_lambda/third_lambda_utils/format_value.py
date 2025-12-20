def format_value(value):
    """
    This function:
        Converts one Python value
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
        # '"O''Rourke"').
        # !r turns value into a
        # Python string literal,
        # which includes double
        # quotes:
        if "'" in value:
            value = value.replace("'", "''")  # 'O''Mally', literal is O''Mally
        return f'"{value}"'  # '"O''Mally"', literal is "O''Mally"
        # or '"David Brent"', literal is "David Brent"

    elif isinstance(value, bool):
        return '"TRUE"' if value else '"FALSE"'
    else:
        return str(value)  # 5-> '5', 3.14->'3.14'
