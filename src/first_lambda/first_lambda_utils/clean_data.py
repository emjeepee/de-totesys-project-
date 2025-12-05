from .make_data_json_safe import make_data_json_safe




def clean_data(table, table_dict):
    """
    This function:
        1. receives a table from the
        totesys database. the table
        will include rows whose 
        field data the totesys 
        database has updated since 
        a the last run of this 
        pipeline.

        2. changes field data in
        the rows such that 
        datetime.datetime objects
        become iso strings and
        decimal.Decimal objects 
        become strings.

        
    args:
        table: the name of a table.

        table_dict: a dictionary 
        whose sole key is the name 
        of a table. The value of 
        the key is a list of 
        dictionaries, each 
        dictionary representing a 
        row of the table. Each
        row contains field data 
        that database totesys has 
        updated since the last run 
        of this pipeline.
 


    returns:
        a dictionary with sole key
        "table", whose value is a list
        of dictionaries, each 
        dictionary representing a 
        row of table table if that row 
        contains updated field data.
        The field data in the row is 
        now clean (ie of the correct 
        type).
        

        """
    
    # change 
    # datetime.datetime objs ->  iso strings
    # decimal.Decimal obj -> strings:
    clean_table_list = make_data_json_safe(table_dict[table]) # [{<updated-row data>}, {<updated-row data>}, etc]
    clean_table_dict = {table: clean_table_list}

    return clean_table_dict
