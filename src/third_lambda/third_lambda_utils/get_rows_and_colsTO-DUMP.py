import duckdb








def get_rows_and_cols(parquet_buffer):
    """
    This function: 
        gets the rows and 
        columns of a table in a Parquet file
    
    
    
    """


    # make a query string for 
    # duckdb so that it can 
    # read into the in-memory
    # duckdb database the 
    # Parquet file in the 
    # BytesIO buffer, creating 
    # a table called 'table':
    read_pq_query = f"""
    CREATE TEMPORARY VIEW table AS 
    SELECT * FROM parquet_scan(?)", 
    (parquet_buffer,)
                    """
        
    # Read the parquet file:
    conn.execute(read_pq_query)

    # get the rows of the 
    # table as a list of 
    # tuples:
    rows = conn.execute("SELECT * FROM table").fetchall() # list of tuples
    
    # Make a list of 
    # column-name strings.
    # conn.description is a 
    # sequence of metadata 
    # tuples that contain info
    # about the columns in 
    # the last query result. 
    # desc[0] is a column name 
    # as a string:
    columns = [desc[0] for desc in conn.description] # list of strings