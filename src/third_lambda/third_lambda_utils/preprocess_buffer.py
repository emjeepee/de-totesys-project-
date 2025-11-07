import duckdb


def preprocess_buffer(buffer, table_name):
    """
    This function:
        creates:
            1) df: a DuckDB dataframe 
            2) cols: 
    
    
    Args:
        buffer: a memory BytesIO buffer
        containing a Parquet file that
        represents a dimension table
        or the fact table. This comes
        from the processed bucket

    Returns:
        this list: [df, cols, cols_str]

    """

    # Open a temp in-memory DuckDB 
    # database (not saved to disk)
    # that DuckDB will use to read 
    # and query the Parquet data:
    con = duckdb.connect(database=':memory:')

    # Make a DuckDB dataframe from 
    # the Parquet file in the 
    # buffer. fetchall() gets all 
    # the rows of data from the 
    # Parquet file as a list of 
    # tuples:
    df = con.execute("SELECT * FROM read_parquet(?)", [buffer]).fetchall()

    # Grab the column names of the 
    # table:
    cols = [desc[0] for desc in con.description] # ['dim_design', 'xxx', 'yyy'])

    # Join the column names with 
    # commas:
    cols_str = ", ".join(cols) # 'dim_design, xxx, yyy'
    
    return [df, cols, cols_str]