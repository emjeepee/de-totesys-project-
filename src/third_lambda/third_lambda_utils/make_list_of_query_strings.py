


from .make_list_of_formatted_row_values import make_list_of_formatted_row_values
from .make_query_string_for_one_row import make_query_string_for_one_row


def make_list_of_query_strings(rows, table_name, column_str):
    """
    This function
        1. generates a list of SQL 
        INSERT statements, each 
        statement will tell the 
        data warehouse to insert 
        values into a particular 
        dimension table or the 
        facts table.
        
        2. returns the list.
    
    Args:
        rows: the rows of a 
        dimension table or the 
        fact table taken from a
        a duckdb database and 
        in the form of a list 
        of tuples, like this: 
        # [
        #   (1,  'xxx',   75.50,   datetime.date(2020, 1, 15) ),
        #   (2,  'yyy',   82.00,   datetime.date(2019, 6, 10) ),
        #   (3,  'zzz',   69.75,   datetime.date(2021, 3, 22) ),
        #    etc
        # ]


        
        table_name: the name of 
        the dimension table or 
        facts table.
        
        column_str: a string 
        containing all of the 
        column names of the 
        dimension table or facts 
        table.

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
        # formatted_values will hold 
        # the formatted values for 
        # one row:
        formatted_values = make_list_of_formatted_row_values(row)
        # make a list of query 
        # strings:
        queries = make_query_string_for_one_row(formatted_values, queries, table_name, column_str)


    return queries
    