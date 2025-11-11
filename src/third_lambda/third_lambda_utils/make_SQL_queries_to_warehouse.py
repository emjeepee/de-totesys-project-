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
        Loops through a list of 
        query strings and for 
        each string queries the 
        warehouse.

    Args:
        1) qrs_list: a python list 
           of SQL query strings.
        2) conn: a pg8000.native 
           Connection object.

    Returns:
        None.

    """

    # Could put this error message and
    # others in other functions in lookup:
    err_Msg = f"Error in make_SQL_queries_to_warehouse()." \
              "An error occured while trying to connect"  \
              "to the data warehouse."  \

    try:
        # Make sql queries to 
        # the warehouse:
        for q_str in qrs_list: conn.run(q_str)
        
    except (DatabaseError, InterfaceError, TimeoutError):
        logger.info(err_Msg)
        raise 

    