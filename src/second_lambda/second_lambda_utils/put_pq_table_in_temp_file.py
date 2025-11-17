import duckdb



def put_pq_table_in_temp_file(table_name: str, col_defs: str, values_list, placeholders, tmp_path: str):
    """
    This function:
        Uses table data (extracted 
        elsewhere from the 
        ingestion bucket and 
        converted into a list of 
        dictionaries) to recreate 
        the table in Parquet 
        format in duckdb. 
        The original table was 
        either a dimension table 
        or the fact table, either 
        in list form.
        
        This function: 
        1. opens a duckdb connection
        2. makes a duckdb table, starting
            with the table columns
        3. inserts the rows of the table             
            into the duckdb table
        4. saves the table in a 
            temporary file path.            
    Args:
        table_name: the name of a 
            dimension table or the 
            fact table. 
        
        col_defs: a string of the 
            column names of the table.
        values_list: a list of lists,
        the member lists containing 
        row values in tring form.
        placeholders: a string of ?s
        equal in number to the number 
        of columns in the table.
        tmp_path: the path to the 
            temporary file.

                    
    Returns:
         None       
    """
    print(f"MY_INFO >>>>> In function put_pq_table_in_temp_file(). About to run conn = duckdb.connect(':memory:')")
    # Create a database in RAM and
    # create object conn to allow
    # interaction with it:
    conn = duckdb.connect(':memory:')

    # make a table, starting with 
    # the column names:
    print(f"MY_INFO >>>>> In function put_pq_table_in_temp_file(). About to run conn.execute with f string with CREATE TABLE. The table is {table_name} and col_defs is {col_defs};")
    conn.execute(f"CREATE TABLE {table_name} ({col_defs});")

    # Insert the table's rows
    # one at a time:
    for values in values_list:
        try:
            conn.execute(f'INSERT INTO {table_name} VALUES ({placeholders})', values)
            conn.execute(f"COPY (SELECT * FROM {table_name}) TO ? (FORMAT PARQUET)", [tmp_path])
        except Exception as e:
            print(f"MY_INFO >>>>> In function put_pq_table_in_temp_file(), in the loop. Exception raised, skipping row {values}: {e}")
    # Write the Parquet file to
    # the temporary file:
    # SELECT * FROM data - get DuckDB to read the table in the list
    # COPY ... TO ? - save the results to a file
    # [tmp_path] - replace ? with your temp file path
    # FORMAT PARQUET - save as Parquet format
        finally:
            # Close the connection to the
            # database:
            print(f"MY_INFO >>>>> In function put_pq_table_in_temp_file(). Out of loop. About to run conn.close() and return None")
            conn.close()
            return None
            


    
