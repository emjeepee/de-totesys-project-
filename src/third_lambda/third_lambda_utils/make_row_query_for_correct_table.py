from src.third_lambda.third_lambda_utils.make_query_for_one_row_fact_table import make_query_for_one_row_fact_table
from src.third_lambda.third_lambda_utils.make_query_for_one_row_dim_table import make_query_for_one_row_dim_table





def make_row_query_for_correct_table(table_name: str, pk_col: str, df_cols, vals_lst: list):
    """
    This function:
        1) determines whether it needs
            to make an SQL query string 
            for a dimension table or 
            the fact table.
        2) carries out 1) above by 
            determining whether the 
            passed-in table name is 
            sales_order (the fact 
            table) or anything else 
            (the dimensions tables).
        
    Args: 
        1) table_name: the name of the
            table. will be 'sales_order' 
            in the case of the fact 
            table or, eg, 'design' for a 
            dimesnion table.
        2) pk_col: the name of the 
            primary key, eg 'design_id'.
        3) df_cols: a list of the 
            column names of the table. 
        4) vals_lst: a list of the 
            values of the row in 
            question.

    Returns:
        An SQL query string that another
         utility function will use to 
         insert or update the row data 
         into the appropriate table in 
         the warehouse.            

    """


    if table_name == 'sales_order':
        sql_query_str = make_query_for_one_row_fact_table(table_name, df_cols, vals_lst)
    else: # table_name is, for example, 'design'
        sql_query_str = make_query_for_one_row_dim_table(table_name, pk_col, df_cols, vals_lst)

    return sql_query_str 

