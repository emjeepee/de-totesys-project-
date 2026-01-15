import duckdb


def put_pq_table_in_temp_file(
    table_name: str, col_defs: str, values_list, placeholders, tmp_path: str
                             ):
    """

    This function:
        1. Uses DuckDB to recreate
         table data in Parquet
         format. The table is
         either a dimension table
         or the fact table,
         extracted by a previous
         function from the
         ingestion bucket and
         converted into a list of
         dictionaries.

        2. opens a DuckDB connection.

        3. makes a duckdb table,
         starting with the table
         columns.

        4. inserts the rows of the
         table into the DuckDB table

        5. saves the table in a
         temporary file path.

        6. is called by function
        convert_to_parquet().


    Args:
        table_name: the name of a
         dimension table or the
         fact table. Its form is
         a list of dictionaries,
         where each dictionary
         represents a row and
         whose key-value pairs
         are
         columnname: fieldvalue
         pairs.

        col_defs: a string of the
         table's column names and 
         the types of their filed 
         data, eg
         'col_name_1 INT, col_name_2 TEXT ...'

        values_list: a list of lists,
         where each member list
         contains the values from one
         row in string form, eg
         [
         ["val_1", "val_2", etc],
         ["val_1", "val_2", etc],
         etc
         ]

        placeholders: a string of
        question marks ('?'s) equal
        in number to the number of
        columns in the table, eg
        '?, ?, ?'

        tmp_path: file path to the
         temporary file.


    Returns:
         None
    """

    # Create a database in RAM and
    # create object conn to allow
    # interaction with it:
    conn = duckdb.connect(":memory:")



    # make SQL strings for duckdb.
    # For making the table:
    make_table_string = f"CREATE TABLE {table_name} ({col_defs});"
    # For inserting the rows:
    insert_rows_string = f"INSERT INTO {table_name} VALUES ({placeholders})"
    # For storing the table in
    # Parquet form in temp file 
    # path:
    put_pq_file_in_tmp_string = (
                             f"COPY (SELECT * FROM {table_name}) "
                             "TO ? (FORMAT PARQUET)"
                            )

    # Use the strings made above:
    for index, values in enumerate(values_list): # values is a list
        if index == 0:
            conn.execute(make_table_string)
        conn.execute(insert_rows_string, values)

    conn.execute(put_pq_file_in_tmp_string, tmp_path)

    # Above: 
    # SELECT * FROM {table_name} - gets
    # DuckDB to read the table in the list.
    # COPY ... TO ? - saves the results to 
    # a file 
    # [tmp_path] - replace ? with the temp 
    # file path
    # FORMAT PARQUET - format to save in

    # Close the connection to
    # the database:
    conn.close()













#--------------------------------------------------------------
# OLD CODE:
    # # make a table, starting with
    # # the column names:
    # conn.execute(f"CREATE TABLE {table_name} ({col_defs});")
    # eg CREATE TABLE staff (staff_id INT, xxx TEXT, yyy INT, zzzz TEXT, etc );

    # # Insert the rows into
    # # the duckdb table:
    # for values in values_list:  # values is a list
    #     conn.execute(f"INSERT INTO {table_name} VALUES ({placeholders})",
    #                  values)

    #     conn.execute(
    #               f"COPY (SELECT * FROM {table_name}) TO ? (FORMAT PARQUET)",
    #               [tmp_path]
    #                 )
#--------------------------------------------------------------