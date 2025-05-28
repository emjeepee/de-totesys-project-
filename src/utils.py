def read_table(table_name, conn, after_time):
    '''
    selects all the data from the specified table after the last run time

    takes the arguments for:
    the name of the table to select from,
    the connection to the database,
    the time which to select records updated after (ie the last time the function was run)

    returns a string to be converted into json
    '''
    result = conn.run(f'''
    SELECT * FROM :table_name
    WHERE last_updated > :after_time
    ''', table_name = table_name, after_time = after_time)

    return result