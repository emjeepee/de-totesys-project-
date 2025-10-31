import os
from pg8000.native import Connection



def conn_to_db(DB_NAME: str):
    
    # OLD:
    # username = os.environ[f"TF_{DB_NAME}_DB_USER"]
    # password = os.environ[f"TF_{DB_NAME}_DB_PASSWORD"]
    # database = os.environ[f"TF_{DB_NAME}_DB_DB"]
    # host = os.environ[f"TF_{DB_NAME}_DB_HOST"]
    # port = os.environ[f"TF_{DB_NAME}_DB_PORT"] # default: 5432?


    username = os.environ[f"TF_VAR_{DB_NAME}_DB_USER"]
    password = os.environ[f"TF_VAR_{DB_NAME}_DB_PASSWORD"]
    database = os.environ[f"TF_VAR_{DB_NAME}_DB_DB"]
    host = os.environ[f"TF_VAR_{DB_NAME}_DB_HOST"]
    port = os.environ[f"TF_VAR_{DB_NAME}_DB_PORT"] # default: 5432?


    conn = Connection(
        username,
        database=database,
        password=password,
        host=host,
        port=port,
        ssl_context=True,
                     )

    return conn


def close_db(conn: Connection):
    conn.close()
