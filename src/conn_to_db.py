import os
from pg8000.native import Connection

# from pg8000.dbapi import Connection as RemoteConnection


def conn_to_db(DB_NAME: str = "TESTDB"):
    username = os.environ["TOTESYS_DB_USER"]
    password = os.environ["TOTESYS_DB_PASSWORD"]
    database = os.environ["TOTESYS_DB_DB"]
    host = os.environ["TOTESYS_DB_HOST"]
    port = os.environ["TOTESYS_DB_PORT"] # default: 5432?
    return Connection(
        username,
        database=database,
        password=password,
        host=host,
        port=port,
        ssl_context=True,
    )


def close_db(conn: Connection):
    conn.close()
