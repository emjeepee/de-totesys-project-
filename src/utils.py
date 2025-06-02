import json


def read_table(table_name, conn, after_time):
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


def convert_data(data):
    try:
        return json.dumps(data)
    except (ValueError, TypeError) as error:
        raise ValueError(f"Data cannot be converted: {error}")
