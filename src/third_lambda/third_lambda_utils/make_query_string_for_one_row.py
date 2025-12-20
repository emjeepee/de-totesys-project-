def make_query_string_for_one_row(formatted_values, table_name, column_str):
    """
    This function:
        Makes an SQL INSERT query
        string for one row of
        a dimension table or the
        facts table.
        Function
        make_list_of_query_strings()
        calls this function in a
        loop.

    Args:
        formatted_values: a list
        of values from one row of
        a table, eg
        ['5', '"xyz"', '75.5', '"TRUE"', '"NULL"']

        table_name: the name of a
        dimension table or the
        facts table.

        column_str: a string that
        contains all of the column
        names of the table in question,
        eg 'Aaa, Bbb, Ccc, Ddd'.

    Returns:
        an SQL INSERT query string.

    """
    # formatted_values is ['5', '"xyz"', '75.5', '"TRUE"', '"NULL"']
    # value_list looks like: '5, "xyx", 75.5, "TRUE", "NULL"'
    value_list = ", ".join(formatted_values)
    query_str = (
        f"INSERT INTO {table_name} ({column_str}) VALUES ({value_list});"
                )

    return query_str
