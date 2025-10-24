

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
        a python list. Each member of the list is a 
         python dictionary that represents a table
         and its updated rows only. 
         Each dictionary has one key, the name of a table.
         The value of the key is a list of dictionaries,
         each of which is an updated row of that table. 

    """
    # read_table() below returns a dictionary, for example
    # {'sales': [{<data from one row>}, {<data from one row>}, etc]},
    # where {<data from one row>} is an updated row.

    data_list = []
    for table in table_names:
        result = read_table(table, conn, after_time)  # {'design': [{<data from one row>}, {<data from one row>}, etc]}
        data_list.append(result)
        # data_list is a python list. Each member of that list is 
        # this type of dictionary: 
        # {'<table_name_here>': [{<data from one row>}, {<data from one row>}, etc]}.
        # The dictionary has one key, the name of a table. 
        # The value of that key is a list of dictionaries, each dictionary representing
        # an updated row of that table. The keys of that dictionary are column names and 
        # their values are the values in that row.
    return data_list
