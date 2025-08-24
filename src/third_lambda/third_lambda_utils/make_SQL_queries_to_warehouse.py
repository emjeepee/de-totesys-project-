from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db


def make_SQL_queries_to_warehouse(qrs_list: list, conn):
    """This function:
        Sends SQL query strings to the warehouse.

    Args:
        1) qrs_list: a python list of SQL query strings.
        2) conn: a pg8000.native Connection object.

    Returns:
        None

    """

    try:
        # send sql queries to warehouse:
        for query_string in qrs_list:
            conn.run(query_string)

        return
    except Exception:
        raise RuntimeError