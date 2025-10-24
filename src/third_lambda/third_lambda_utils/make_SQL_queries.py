from .make_query_for_one_row_fact_table import make_query_for_one_row_fact_table
from .make_query_for_one_row_dim_table  import make_query_for_one_row_dim_table
from .make_row_query_for_correct_table  import make_row_query_for_correct_table


def make_SQL_queries(df, table_name: str):
    """
    This function:
        1) reads the data in a pandas dataFrame.
        2) creates an SQL query string for each row 
            of the table in the dataframe. The 
            query has one form if the table is a 
            dimension table and another form if it 
            is the fact table (because the 
            warehouse wants dimension tables whose 
            rows update but requires a fact table 
            that includes historical rows, ie the 
            fact table will have several rows with 
            identical values for primary key 
            sales_order_id but different values 
            under other columns). 
        3) puts the SQL query strings into a python 
            list.
        4) gets called by third_lambda_handler().

    Args:
        1) table_name: a string, the name of a table.
            will be 'sales_order' in the case of the 
            table, otherwise 'design' for example.
        2) df: a pandas DataFrame containing the 
            data for a table.

    Returns:
        A python list of strings, where each string 
        is an SQL query to make to the appropriate 
        table in the data warehouse.
    """
    

    # make a list to hold the 
    # SQL query strings:
    sql_query_list = []

    # Convert each row of the dataFrame
    # to an INSERT type of SQL query 
    # string and put all the strings in
    # sql_query_list. iterrows() loops 
    # through a DataFrame row by row 
    # and for each row returns the 
    # index of the row and the row 
    # itself as a pandas Series (WON'T
    # WORK without index below, so 
    # don't remove!!!):
    for index, row_data in df.iterrows(): # iterrows() is a pandas method 
        # data_frame.columns below is 
        # ['aaa', 'bbb', 'ccc'].

        # Make a list of strings of the values 
        # of a row, converting None to "NULL", 
        # True to "TRUE" and "False" to "FALSE":
        vals_lst = [str(val)    if val is not None else "NULL"      for val in row_data]
        vals_lst = [val         if val is not 'True' else "TRUE"    for val in vals_lst ]
        vals_lst = [val         if val is not 'False' else "FALSE"  for val in vals_lst ]        
        # Get the primary key name (eg 
        # 'design_id'). The first column 
        # in df is always the primary 
        # key because it is in the 
        # Parquet file and the Python 
        # list before that:
        pk_col = df.columns[0]

        # work out whether the table is
        # a dimension table or the fact 
        # table and create the 
        # appropriate SQL query string:
        sql_query_str = make_row_query_for_correct_table(table_name, pk_col, df.columns, vals_lst)
        
        
        # to the list add the query just 
        # made:
        sql_query_list.append(sql_query_str)

    return sql_query_list














        # # make this a function:
        # sql_query_str = (
        #    make_query_for_one_row_fact_table(table_name, df.columns, vals_lst)
        #     if table_name == 'sales_order'
        #     else make_query_for_one_row_dim_table(table_name, pk_col, df.columns, vals_lst)
        #                 )


#         sql_query_str = make_query_for_one_row_fact_table(table_name, df.columns, vals_lst) if table_name == 'sales_order' else make_query_for_one_row_dim_table(table_name, pk_col, df.columns, vals_lst)


# if table_name == 'sales_order':
        #     sql_query_str = make_query_for_one_row_fact_table
        # else:     
        #     sql_query_str = make_query_for_one_row_dim_table(table_name, pk_col, df.columns, vals_lst)
