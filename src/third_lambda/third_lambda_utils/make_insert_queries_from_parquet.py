import duckdb

from .read_parquet_from_buffer   import read_parquet_from_buffer
from .make_list_of_query_strings import make_list_of_query_strings

def make_insert_queries_from_parquet(parquet_buffer, table_name):
    """
    This function:
        generates a list of 
        PostgreSQL INSERT queries.
        Each query relates to a 
        row of data in a table 
        passed into this function 
        in the form of a Parquet 
        file in a BytesIO buffer.
    
    Args:
        parquet_buffer: a BytesIO 
        buffer containing a 
        dimension or fact table 
        in Parquet form.

        table_name: Name of the 
        table in the data warehouse
        that this function must 
        insert data into. 

    Returns:
        list: a list of SQL INSERT 
        query strings.
    """
    
        
    # make an in-memory 
    # duckdb database:
    conn = duckdb.connect(':memory:')

    # Get the duckdb connection 
    # object, the list of 
    # column name strings, 
    # a string that contains all 
    # column names and all of 
    # the rows in the table:
    column_str, rows = read_parquet_from_buffer(parquet_buffer, conn)

    # generate INSERT queries 
    # and put them in a list: 
    queries = make_list_of_query_strings(rows, table_name, column_str)

    # close duckdb in-memory
    # database:
    conn.close()

    return queries



