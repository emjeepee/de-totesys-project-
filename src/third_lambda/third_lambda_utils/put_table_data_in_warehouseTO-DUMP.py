from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db


def put_table_data_in_warehouse(query_list):
    """
    This function:
        1) opens a connection to the warehouse database
        2) loops through a list of SQL query strings and makes
            the query to the warehouse database
        2) closes the connection to the warehouse database

    Args:
        query_list: a list of strings, where each string is an SQL
            query to be made to the warehouse database
        conn_util: the utility function that makes the connection to
            the warehouse database (and returns an instance of a
            pg8000 Connection object)

    Returns:
        None
    """

    # make the connection to the 
    # postgresql warehouse database:
    conn = conn_to_db("WAREHOUSE")

    # send sql queries to the warehouse
    # database:
    for query_string in query_list:
        conn.run(query_string)

    # close the connection to
    # the warehouse:
    close_db()
