import os
from pg8000.native import Connection



def conn_to_db(DB_NAME: str = "TESTDB"):
    
    username = os.environ[f"{DB_NAME}_DB_USER"]
    password = os.environ[f"{DB_NAME}_DB_PASSWORD"]
    database = os.environ[f"{DB_NAME}_DB_DB"]
    host     = os.environ[f"{DB_NAME}_DB_HOST"]
    port     = os.environ[f"{DB_NAME}_DB_PORT"] # default: 5432?
    return Connection(
        username=username,
        database=database,
        password=password,
        host=host,
        port=port,
        ssl_context=True,
    )


def close_db(conn: Connection):
    conn.close()
