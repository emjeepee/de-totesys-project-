import json
from pg8000.native import Connection

def read_table(table_name:str, conn:Connection, after_time:str):
    """
    selects all the data from the specified table after the last run time

    takes the arguments for:
    the name of the table to select from,
    the connection to the database,
    the time which to select records updated after (ie the last time the function was run)

    returns a dict in the format {table_name: <string to be converted into json>}
    """
    result = conn.run(
        f"""
        SELECT * FROM {table_name}
        WHERE last_updated > :after_time
        """,
        after_time=after_time,
    )

    return {table_name: result}


def convert_data(data:dict|list):
    """
    converts the data passed in into json format

    takes the argument for:
    the data, in string form, ready to convert to json

    returns a json object, ready to upload into the bucket as a json file
    """
    try:
        return json.dumps(data)
    except (ValueError, TypeError) as error:
        raise ValueError(f"Data cannot be converted: {error}")
