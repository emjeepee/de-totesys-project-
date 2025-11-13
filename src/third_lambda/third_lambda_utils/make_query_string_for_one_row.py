





def make_query_string_for_one_row(formatted_values, queries, table_name, column_str):
    """
    This function:
        Makes an SQL query string
        from the column names and 
        the values of one row of 
        a table and appends the 
        string to a pre-existing
        list of query strings.
    
    Args:
        formatted_values: a list 
        of values from one row of 
        a table, eg 
        [5, 'xyx', 75.5, '2020-01-15'].

        queries: a list of SQL
        query strings to which this 
        function adds.

        table_name: the name of a 
        dimension table or the 
        facts table.

        column_str: a string that 
        contains all of the column
        names of the table in question,
        eg 'Aaa, Bbb, Ccc, Ddd'. 
    
    Returns:
        a list that contains SQL 
        query strings.         
    
    """

    value_list = ', '.join(formatted_values) # "5, 'xyx', 75.5, '2020-01-15'"
    query = f'INSERT INTO "{table_name}" ({column_str}) VALUES ({value_list});'
    queries.append(query)

    return queries