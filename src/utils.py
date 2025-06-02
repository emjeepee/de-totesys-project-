import json
from pg8000.native import Connection
import datetime

def read_table(table_name:str, conn:Connection, after_time:str):
    """
    selects all the data from the specified table after the last run time

    takes the arguments for:
    the name of the table to select from,
    the connection to the database,
    the time which to select records updated after (ie the last time the function was run)

    returns a string to be converted into json
    """
    # try:
    result = conn.run(
        f"""
        SELECT * FROM :table_name
        WHERE last_updated > :after_time
        """,
        table_name=table_name,
        after_time=after_time,
    )

    return result
    # except Exception:
    #     raise ValueError


def convert_data(data: dict | list):
    def serialize_datetime(obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()  # Convert datetime
        return obj
    try:
        return json.dumps(data, default=serialize_datetime)
    except (ValueError, TypeError) as error:
        raise ValueError(f"Data cannot be converted: {error}")
