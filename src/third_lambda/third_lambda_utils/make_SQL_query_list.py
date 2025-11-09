from .preprocess_buffer_1       import preprocess_buffer
from .make_vals_for_SQL_query import make_vals_for_SQL_query
from .make_SQL_query          import make_SQL_query




def make_SQL_query_list(buffer, table_name):
    """
    This function:
        Makes a list of SQL queries from 
        all rows of a dimension table or 
        the fact table. 

    Args:
        buffer: a IOBytes object that 
        contains a Parquet file that 
        represents the dimension table
        or fact table.

    Returns:
        A list of strings, each string 
        being an SQL query. The number 
        of strings is equal to the 
        number of rows in the dimension 
        table or fact table.          
    """


    lst = preprocess_buffer(buffer, table_name)
    queries = []

    for row in lst[0]:
        # Make that part of the query 
        # string that contains the 
        # values of the row:
        vals_str = make_vals_for_SQL_query(row)

        # Add the rest of the SQL 
        # query string:
        query    = make_SQL_query(table_name, lst[2], vals_str, lst[1])
        queries.append(query)
    
    return queries
