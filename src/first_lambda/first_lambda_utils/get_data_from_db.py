from .make_data_json_safe import make_data_json_safe
from .clean_data import clean_data



def get_data_from_db(table_names: list, after_time: str, conn, read_table):
    """
    This function:
        1) loops through a list of 
            names of all tables in 
            the ToteSys database
        2) in the loop, for each 
           table name gets the 
           table from the totesys 
           database if the database 
           has updated the field 
           data in any row of that 
           table after passed-in
           time after_time. 
        3) creates a list of such 
           tables.
        4) returns that list.            

    Args:
        table_names: a list of 
            strings, each being the 
            name of a table in the 
            totesys database, eg 
            'sales_orders'.
        
        after_time: for each table 
            in the totesys database 
            get_data_from_db() gets 
            rows if totesys has updated
            any of their field data 
            after time after_time. 
        
        conn: an instance of pg8000's 
            Connection object
        
        read_table: a function that reads 
            the totesys database and gets 
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
        dictionary are 
        columnname-fieldvalue pairs. 
        The list takes this form:
         [
         {'design': [{<updated-row data>}, {<updated-row data>}, etc]},
         {'staff': [{<updated-row data>}, {<updated-row data>}, etc]},
                etc, etc
         ]

    """

    data_list = []
    for table in table_names:
        table_dict = read_table(table, conn, after_time)  # {'design': [{<updated-row data>}, {<updated-row data>}, etc]}
        clean_table_dict = clean_data(table, table_dict)
        data_list.append(clean_table_dict) # [{'design': [{<updated-row data>}, etc]}, {'sales': [{<updated-row data>}, etc]}, etc].
                                 # where {<updated-row data>} is eg {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}
    return data_list
