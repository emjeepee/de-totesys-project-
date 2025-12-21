from pg8000.native import DatabaseError

import logging

from .errors_lookup import errors_lookup


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

    # Make SQL queries to
    # the warehouse:
    try:
        for q_str in qrs_list:
            conn.run(q_str)

    except DatabaseError:
        # log, stop code:
        logger.exception(errors_lookup["err_1"])
        raise
