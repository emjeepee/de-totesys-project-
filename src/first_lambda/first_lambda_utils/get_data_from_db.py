

def get_data_from_db(table_names: list, after_time: str, conn, read_table):
    """
    This function:
        1) loops through a list of names of all
            tables in the ToteSys database
        2) in the loop calls read_table() for 
            each table name to get updated rows 
            for that table from the ToteSys 
            database.
            'Updated' here means modified in the 
            ToteSys database after passed-in 
            time after_time.
            read_table() returns a dictionary.
        3) appends each updated row (which is 
            in the form of a dictionary) to
            a list.
        4) returns that list of dictionaries.            

    Args:
        table_names: a list of strings, each string
                being the name of a table in the
                ToteSys database, eg 'sales'.
        after_time: for each table in the ToteSys 
                database get_data_from_db() gets rows
                that contain data that have been 
                modified after after_time. 
        conn: an instance of pg8000's Connection object
        read_table: a utility that this function
                employs to read the ToteSys database 
                and get a table's updated rows from it.

    Returns:
        a python list of dictionaries, each dictionary 
         representing a table and its updated rows only. 
         Each dictionary has one key, the name of a table.
         The value of the key is a list of dictionaries,
         each of those dictionaries representing an 
         updated row of that table. The keys of that 
         dictionary are the column names and the values 
         are the row values under those columns.

    """

    data_list = []
    for table in table_names:
        result = read_table(table, conn, after_time)  # {'design': [{<updated-row data>}, {<updated-row data>}, etc]}
        
        data_list.append(result) # [{'design': [{<updated-row data>}, etc]}, {'sales': [{<updated-row data>}, etc]}, etc].
                                 # where {<updated-row data>} is eg {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}
    return data_list
