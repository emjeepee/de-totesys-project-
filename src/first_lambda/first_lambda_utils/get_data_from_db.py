from .make_data_json_safe import make_data_json_safe




def get_data_from_db(table_names: list, after_time: str, conn, read_table):
    """
    This function:
        1) loops through a list of 
            names of all tables in 
            the ToteSys database
        2) in the loop calls 
            read_table() for each 
            table name to get the 
            updated rows for that 
            table from the ToteSys 
            database, where 'updated' 
            means modified in the 
            ToteSys database after 
            passed-in time 
            after_time.
        3) appends each updated row 
            (which is in the form of 
            a dictionary) to a list.
        4) returns that list of 
           dictionaries.            

    Args:
        table_names: a list of 
            strings, each string
            being the name of a 
            table in the ToteSys 
            database, eg 'sales_orders'.
        
        after_time: for each table 
            in the ToteSys database 
            get_data_from_db() gets 
            rows that contain data 
            that have been modified 
            after after_time. 
        
        conn: an instance of pg8000's 
            Connection object
        
        read_table: a function that reads 
            the ToteSys database and gets 
            a table's updated rows from 
            it.

    Returns:
        a list of dictionaries, each 
        dictionary containing only 
        the updated rows of a table. 
        Each dictionary has one key, 
        the name of a table.
        The value of the key is a 
        list of dictionaries, each 
        of which represents an 
        updated row of that table. 
        The key-value pairs of that 
        dictionary are columnname-
        fieldvalue pairs. The 
        list takes this form:
         [
         {'design': [{<updated-row data>}, {<updated-row data>}, etc]},
         {'staff': [{<updated-row data>}, {<updated-row data>}, etc]},
                etc, etc
         ]

    """

    data_list = []
    for table in table_names:
        table_dict = read_table(table, conn, after_time)  # {'design': [{<updated-row data>}, {<updated-row data>}, etc]}
        
        # change 
        # datetime.datetime objects 
        # to iso strings and 
        # decimal.Decimal objects 
        # to strings:
        clean_table_list = make_data_json_safe(table_dict[table]) # [{<updated-row data>}, {<updated-row data>}, etc]
        clean_table_dict = {table: clean_table_list}
        data_list.append(clean_table_dict) # [{'design': [{<updated-row data>}, etc]}, {'sales': [{<updated-row data>}, etc]}, etc].
                                 # where {<updated-row data>} is eg {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}
    return data_list
