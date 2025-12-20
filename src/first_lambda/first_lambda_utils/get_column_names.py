from pg8000.native import Error

import logging

from .errors_lookup import errors_lookup


logger = logging.getLogger(__name__)


def get_column_names(conn_obj, table_name: str):
    """
    This function:
        Makes an SQL query to the totesys
         database to get the column names
         of a table.

    Args:
        1) conn_obj: an instance of the
         pg8000.native Connection class.
        2) table_name: the name of the
         table in the ToteSys database
         from which to get its column
         names.

    Returns:
        A list of lists. Each member list
         contains one string, which is the
         name of a column.

    """

    query = (
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
            )

    try:
        response = conn_obj.run(query)
        return response

    except Error:
        # log the error
        # and stop the code:
        logger.exception(
            errors_lookup["err_2"] + f"{table_name}"
        )  # <-- logs full stacktrace
        raise
