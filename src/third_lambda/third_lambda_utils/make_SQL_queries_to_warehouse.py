from pg8000.native import (
    DatabaseError,
    InterfaceError,
    TimeoutError    
                         )

import logging


logger = logging.getLogger(__name__)


def make_SQL_queries_to_warehouse(qrs_list: list, conn):
    """
    This function:
        Sends SQL query strings to the warehouse.

    Args:
        1) qrs_list: a python list of SQL query strings.
        2) conn: a pg8000.native Connection object.

    Returns:
        None

    """


    err_Msg = f"Error in make_SQL_queries_to_warehouse()." \
              "An error occured while trying to connect"  \
              "to the data warehouse."  \

    try:
        # send sql queries to warehouse:
        for query_string in qrs_list:
            conn.run(query_string)
        
    except (DatabaseError, InterfaceError, TimeoutError):
        logger.info(err_Msg)
        raise 
