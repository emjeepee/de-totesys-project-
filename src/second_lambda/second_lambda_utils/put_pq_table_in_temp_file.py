import duckdb



def put_pq_table_in_temp_file(table_name: str, col_defs: str, values_list, placeholders, tmp_path: str):
    """
    function convert_to_parquet()
    calls this function, which:
        1. Uses duckdb to recreate 
         the table data in Parquet 
         format. The table is 
         either a dimension table 
         or the fact table, 
         extracted by a previous 
         function from the 
         ingestion bucket and 
         converted into a list of 
         dictionaries. 
        
        2. does 1. above by opening
         a duckdb connection
        
        3. making a duckdb table, 
         starting with the table 
         columns
        
        4. inserting the rows of the 
         table into the duckdb table
        
        5. saving the table in a 
         temporary file path.  


    Args:
        table_name: the name of a 
         dimension table or the 
         fact table. 
        
        col_defs: a string of the 
         column names of the table,
         eg 'col_1, col_, col_3 ...'.

        values_list: a list of lists,
         where each member list 
         contains the values from one
         row in string form, eg.
     
        placeholders: a string of 
        question marks ('?'s) equal 
        in number to the number of 
        columns in the table.
        
        tmp_path: file path to the 
         temporary file.

                    
    Returns:
         None       
    """

    # Create a database in RAM and
    # create object conn to allow
    # interaction with it:
    conn = duckdb.connect(':memory:')

    # make a table, starting with 
    # the column names:
    conn.execute(f"CREATE TABLE {table_name} ({col_defs});") # fri21Nov25: problem here
            # eg CREATE TABLE staff (department_name TEXT, location TEXT, staff_id ???, first_name TEXT, last_name TEXT, email_address TEXT);

    # Insert the table's rows
    # one at a time:
    for values in values_list:
            conn.execute(f'INSERT INTO {table_name} VALUES ({placeholders})', values)
            conn.execute(f"COPY (SELECT * FROM {table_name}) TO ? (FORMAT PARQUET)", [tmp_path])
        # Write the Parquet file to
        # the temporary file:
        # SELECT * FROM data - get DuckDB to read the table in the list
        # COPY ... TO ? - save the results to a file
        # [tmp_path] - replace ? with your temp file path
        # FORMAT PARQUET - save as Parquet format
            # Close the connection to the
            # database:
            conn.close()
    
    return None
            


    
