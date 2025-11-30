import os
import logging

from pg8000.native import Connection, Error


from .errors_lookup import errors_lookup

logger = logging.getLogger(__name__)



def conn_to_db(DB_NAME: str):
    

    user     = os.environ[f"{DB_NAME}_DB_USER"]
    password = os.environ[f"{DB_NAME}_DB_PASSWORD"]
    database = os.environ[f"{DB_NAME}_DB_DB"]
    host     = os.environ[f"{DB_NAME}_DB_HOST"]
    port     = os.environ[f"{DB_NAME}_DB_PORT"] # default: 5432?

    try:
        conn = Connection(
            user=user,
            password=password,
            database=database,
            host=host,
            port=port,
            ssl_context=True,
                        )
   
    except Error:
        # log exception 
        # and stop code:
        logger.exception(errors_lookup['err_8'])
        raise

    return conn





def close_db(conn: Connection):
    try:
        conn.close()

    except Error:
        # log the error 
        # and stop the code:
        logger.exception(errors_lookup['err_9'])  # <-- logs full stacktrace
        raise
