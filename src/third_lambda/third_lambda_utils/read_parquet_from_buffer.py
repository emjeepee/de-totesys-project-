import tempfile



def read_parquet_from_buffer(parq_in_buffer, conn):
    """
    This function:
        1. creates a string of 
        column names so that 
        the string can become 
        part of an SQL INSERT 
        query in another 
        function.

        2. creates a list of 
        tuples for use in 
        another function where 
        each tuple contains the 
        values of one row.

            
    Args:
        parq_in_buffer: a dimension 
        table or the fact table 
        table in Parquet form in a 
        BytesIO buffer.

        conn: a duckdb connection 
        object.

        
    Returns:
        result: a DuckDB query 
        result object.

    """

    parq_in_buffer.seek(0)
    
    # Write BytesIO to a 
    # temp file:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        tmp.write(parq_in_buffer.getvalue())
        tmp.flush()  # Ensure all bytes are written

        # Now pass the file path 
        # (string) to DuckDB
        # to read the parquet 
        # data:
        result = conn.execute(
            "SELECT * FROM parquet_scan(?)",
            [tmp.name]
                             )
    
    return result        
