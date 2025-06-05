import json
from pg8000.native import Connection
import datetime
import decimal
import logging


logger = logging.getLogger("MyLogger")


def read_table(table_name: str, conn: Connection, after_time: str):
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
        WHERE last_updated > :after_time LIMIT 20;
        """,
        after_time=after_time,
    )

    # trials
    query_result = conn.run(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
    )
    column_names = [col[0] for col in query_result]
    dict_data = []
    for i in result:
        if isinstance(i, tuple):
            i = list(i)
        if isinstance(i, list):
            dic_to_append = {}
            for j in range(len(i)):
                if isinstance(i[j], datetime.datetime):
                    dic_to_append[column_names[j]] = i[j].isoformat()
                else:
                    dic_to_append[column_names[j]] = i[j]
            dict_data.append(dic_to_append)

    return {table_name: dict_data}  # result


def convert_data(data: dict | list):
    """
    converts the data passed in into json format

    takes the argument for:
    the data, in string form, ready to convert to json

    returns a json object, ready to upload into the bucket as a json file
    """

    try:
        return json.dumps(data, default=serialize_datetime)
    except (ValueError, TypeError) as error:
        logger.error("Unable to dump the data")
        raise ValueError(f"Data cannot be converted: {error}")


def serialize_datetime(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()  # Convert datetime
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    return super(obj)
