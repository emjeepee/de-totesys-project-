import os
from pg8000.native import Connection

# from pg8000.dbapi import Connection as RemoteConnection
from dotenv import load_dotenv


def conn_to_db(DB_NAME="TESTDB"):
    load_dotenv()
    username = os.environ[f"PG_{DB_NAME}_USERNAME"]
    password = os.environ[f"PG_{DB_NAME}_PASSWORD"]
    database = os.environ[f"PG_{DB_NAME}_DATABASE"]
    host = os.environ[f"PG_{DB_NAME}_HOST"]
    port = os.environ[f"PG_{DB_NAME}_PORT"]  # default: 5432?
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
