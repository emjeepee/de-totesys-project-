# from .make_data_json_safe import make_data_json_safe
from .clean_data import clean_data


def get_data_from_db(table_names: list, 
                     after_time: str, 
                     conn, 
                     read_table
                     ):
    """
    This function:
        1) loops through a list of
            names of all tables in
            database toteSys

        2) in a loop gets those 
           rows of each table that 
           contain field data 
           updated after passed-in 
           time after_time.

        3) creates a list of such
           tables.

        4) returns that list.

    Args:
        table_names: a list of
            strings, each the name of 
            a table in database totesys, 
            eg 'sales_orders'.

        after_time: if after this time 
            database totesys has either 
            inserted new data into rows 
            in the table or has modified 
            existing rows there, this 
            function gets those rows.

        conn: an instance of a 
            pg8000.native Connection 
            object.

        read_table: a function that reads
            the totesys database and gets
            updated or new rows a table.

    Returns:
        a list of dictionaries, each
        dictionary containing only
        updated/new rows of a table.
        Each dictionary has one key,
        the name of a table. 
        The value of the key is a
        list of dictionaries, each
        dictionary representing an
        updated/new row of that table.
        The key-value pairs of each
        dictionary are
        columnname-fieldvalue pairs.
        The list takes this form:
         [
         {'design': [{<clean updated-row>}, {<clean updated-row>}, etc]},
         {'staff': [{<clean updated-row>}, {<clean updated-row>}, etc]},
                etc
         ]

    """

    # Note: code could alternatively 
    # generate list dirty_tables 
    # like this:
    # dirty_tables = list(map(lambda table: read_table(table, conn, after_time), 
    #                     table_names))
    dirty_tables = [
                    read_table(table, conn, after_time)
                    for table in table_names
                   ]
    # dirty_tables is a list
    # of dictionaries like:
    # [
    #  {'design': [{<unclean-but-updated-row data>}, etc]},
    #  {'sales': [{<unclean-but-updated-row data>}, etc]},
    #  etc
    # ].

    # below next(iter(table_dict)
    # returns the sole key (ie the
    # table name) from dict
    # table_dict:
    clean_tables = [
                    clean_data( next(iter(table_dict)), table_dict )
                    for table_dict in dirty_tables
                   ]
    
    return clean_tables










# OLD CODE:
    # data_list = []
    # for table in table_names:
    #     table_dict = read_table(
    #                             table,
    #                             conn,
    #                             after_time
    #                            )  # {'design': 
    #                               # [{<updated-row data>}, 
    #                               # {<updated-row data>}, 
    #                               # etc]}

    #     clean_table_dict = clean_data(table, table_dict)
        
    #     data_list.append(clean_table_dict)
    #     # data_list becomes: [{'design': [{<updated-row data>}, etc]},
    #     #                     {'sales': [{<updated-row data>}, etc]}, etc].
    #     # where {<updated-row data>} is, eg,
    #     # {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}
    
    # return data_list
