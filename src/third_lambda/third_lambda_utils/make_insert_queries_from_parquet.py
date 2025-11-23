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
        insert data into, eg
        'dim_design' or 
        'fact_sales_orders'. 

    Returns:
        list: a list of SQL INSERT 
        query strings.
    
    """
    
    # make an in-memory 
    # duckdb database:
    conn = duckdb.connect(':memory:')

    # get the rows and columns
    # from the parquet table:
    rows_cols = read_parquet_from_buffer(parquet_buffer, conn) # [column_str, rows]

    # generate INSERT query  
    # strings and put them in 
    # a list: 
    queries = make_list_of_query_strings(rows_cols[1], table_name, rows_cols[0])

    # close duckdb in-memory
    # database:
    conn.close()

    return queries





