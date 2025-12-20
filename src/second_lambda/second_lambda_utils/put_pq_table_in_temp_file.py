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
         column names of the table,
         eg
         'col_name_1, col_name_2, etc'.

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

    # make a table, starting with
    # the column names:
    conn.execute(f"CREATE TABLE {table_name} ({col_defs});")
    # eg CREATE TABLE staff  /
    # (design_id INT, /
    # xxxx TEXT, yyyy INT, /
    # zzzzz TEXT, /
    # etc );

    # Insert the table's rows:
    for values in values_list:  # for each list
        conn.execute(
            f"INSERT INTO {table_name} VALUES ({placeholders})", values
            )

        conn.execute(
            f"COPY (SELECT * FROM {table_name}) TO ? (FORMAT PARQUET)",
            [tmp_path]
                    )
    # Write the Parquet file to
    # the temporary file:
    # SELECT * FROM {table_name} - get
    # DuckDB to read the table in
    # the list.
    # COPY ... TO ? - save the
    # results to a file
    # [tmp_path] - replace ? with the
    # temp file path
    # FORMAT PARQUET - save in Parquet
    # format

    # Close the connection to
    # the database:
    conn.close()
