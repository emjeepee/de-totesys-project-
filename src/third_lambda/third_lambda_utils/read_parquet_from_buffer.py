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

        2. creates a list of tuples
        for use in another function 
        where each tuple contains
        the values of one row.

    
    Args:
        parq_in_buffer: a dimension 
        table or the fact table 
        table in Parquet form in a 
        BytesIO buffer.

        conn: a duckdb connection 
        object.

    Returns:
        list [column_str, rows],
        where: 
            column_str: a string 
            containing all column names, 
            eg
            '"col_1_name", "col_2_name", "col_3_name"'
            
            rows: a list of tuples, each 
             tuple containing a row's values.
    """

    parq_in_buffer.seek(0)
    
    # Write BytesIO to a 
    # temp file:
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        tmp.write(parq_in_buffer.getvalue())
        tmp.flush()  # Ensure all bytes are written

        # Now pass the file path (string) to DuckDB
        # to read the parquet data:
        result = conn.execute(
            "SELECT * FROM parquet_scan(?)",
            [tmp.name]
                             )

    # result.description is 
    # info about each column 
    # (name, type, etc) and 
    # desc[0] is the column name.
    # Get the column names:
    columns = [desc[0] for desc in result.description]
    # ["xxx", "yyy", "zzz", "abc"]

    # make a string of all column 
    # names with the names in double
    # quotes:
    column_str = ', '.join([f'"{col}"' for col in columns])
    # '"xxx", "yyy", "zzz", "abc"'
    
    # get all of the rows
    # as a list of tuples:
    rows = result.fetchall()
        # [
        #   (1,  'xxx',   75.50,   None, True ),
        #   (2,  'yyy',   82.00,   None, False), 
        #   (3,  'zzz',   69.75,   None, True ),
        #    etc
        # ]

    return [column_str, rows]