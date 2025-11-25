import os
from pg8000.native import Connection



def conn_to_db(DB_NAME: str):
    

    user     = os.environ[f"{DB_NAME}_DB_USER"]
    password = os.environ[f"{DB_NAME}_DB_PASSWORD"]
    database = os.environ[f"{DB_NAME}_DB_DB"]
    host     = os.environ[f"{DB_NAME}_DB_HOST"]
    port     = os.environ[f"{DB_NAME}_DB_PORT"] # default: 5432?


    conn = Connection(
        user=user,
        password=password,
        database=database,
        host=host,
        port=port,
        ssl_context=True,
                     )

    return conn


def close_db(conn: Connection):
    conn.close()
