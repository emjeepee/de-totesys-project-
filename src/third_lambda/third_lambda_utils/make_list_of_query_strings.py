

from .read_parquet_from_buffer          import read_parquet_from_buffer
from .make_list_of_formatted_row_values import make_list_of_formatted_row_values
from .make_query_string_for_one_row     import make_query_string_for_one_row


def make_list_of_query_strings(rows, table_name, column_str):
    """
    This function
        1. generates a list of SQL 
        INSERT statements, each 
        statement will tell the 
        data warehouse to insert 
        values into a dimension
        table or the facts table.
        
        2. returns the list.
    
    Args:
        rows: the rows of a 
        dimension table or the 
        fact table taken from a
        a duckdb database and 
        in the form of a list 
        of tuples, like this: 
        # [
        #   (1,  "xxx",   75.50,   None, True ),
        #   (2,  "yyy",   82.00,   None, False),
        #   (3,  "zzz",   69.75,   None, True),
        #    etc
        # ]


        
        table_name: the name of 
        the dimension table or 
        facts table.
        
        column_str: a string 
        containing all of the 
        column names of the 
        dimension table or facts 
        table, eg:
        'some_id, col_a, col_b, col_c'.

    Returns:
        A list of SQL query 
        strings that another 
        function will employ to 
        insert row data from a 
        dimension table or from 
        the facts table into the 
        data warehouse.       

    """

    

    # generate INSERT queries 
    # and put them in a list: 
    queries = []
    for row in rows:
        # form_vals_list will be 
        # a list containing the 
        # formatted values for 
        # one row, like this:
        # ['5', '"xyz"', '75.5', '"TRUE"', '"NULL"']
        form_vals_list = make_list_of_formatted_row_values(row) # ['5', '"xyz"', '75.5', '"TRUE"', '"NULL"']
        # make a list of query 
        # strings:
        query_str = make_query_string_for_one_row(form_vals_list, table_name, column_str)
        queries.append(query_str)

    return queries
    