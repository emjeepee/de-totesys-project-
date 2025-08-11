from src.third_lambda.third_lambda_utils.make_SQL_qry_for_one_row import make_SQL_qry_for_one_row


def make_SQL_queries(data_frame, table_name: str):
    """
    This function:
        1) reads the data in a pandas dataFrame.
        2) creates an SQL query string for each row 
            of the table in the dataframe. The 
            query has one form is the table is a 
            dimension table and another form if it 
            is the fact table (because the dimension 
            tables in the warehouse have to have 
            rows updated whereas the fact table in 
            the warehouse has to include historical
            rows). 
        3) puts the SQL query strings into a python 
            list.


    Args:
        table_name: a string, the name of a table.
        dataFrame: a pandas DataFrame containing the 
         data for a table.

    Returns:
        A python list of strings, where each string 
        is an SQL query to make to the appropriate 
        table in the data warehouse (which another
        function will carry out).
    """
    # make a list to hold the 
    # SQL query strings:
    sql_query_list = []

    # Convert each row to an
    # INSERT sql query string
    # and put all strings in
    # the list. iterrows() is
    # a pandas method that
    # loops through a DataFrame
    # row by row and for each row
    # returns the index of the
    # row and the row itself as
    # a pandas Series (WON'T
    # WORK without index
    # below, so don't remove!!!):
    for index, row in data_frame.iterrows():
        columns = ", ".join(data_frame.columns)
        values = ", ".join(f"'{str(v)}'" if v is not None else "NULL" for v in row)

        # The following line works because 
        # the first column in the dataFrame 
        # is always the primary key (because
        # it is in the Parquet file and 
        # the Python list before that).
        pk_col = data_frame.columns[0]

        # The following function returns 
        # a different SQL query depending
        # on the value of table_name:
        sql_query_str = make_SQL_qry_for_one_row(table_name, pk_col, columns, values, row)

        sql_query_list.append(sql_query_str)

    return sql_query_list




