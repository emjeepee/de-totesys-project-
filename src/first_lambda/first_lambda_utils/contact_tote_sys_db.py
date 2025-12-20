from pg8000 import ProgrammingError


def contact_tote_sys_db(conn_obj, opt: int, after_time: str, table_name: str):
    """
    This function:
        Makes an SQL query to the ToteSys
         database, the nature of which
         depends on the value of opt.

    Args:
        1) conn_obj: an instance of the
         pg8000.native Connection class.

        2) opt: an int the value of which
         determines which SQL query this
         function makes to the ToteSys
         database.
         when opt is 1 the query string
          is to get updated rows.
         when opt is 2 the query string
          is to get column names.

        3) after_time: a time stamp that will
          always be the time of the last run
          of the first lambda function.

        4) table_name: the name of the
         table that this function wants
         to ask the ToteSys database
         about.

    Returns:
        The result of the SQL query this
        function made to the ToteSys database.

    """

    err_Msg = (
        "Error in function contact_tote_sys_db()."
        "\nFailed to read ToteSys database"
        "\neither when:"
        "\n  1) trying to get a list of column "
        "\n      names of a table or"
        "\n  2) trying to get a list of updated"
        "\n     rows for a table."
    )

    if opt == 2:
        query = (
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' ORDER BY ordinal_position"
                )

    if opt == 1:
        query = (
            f"SELECT * FROM {table_name} "
            "WHERE last_updated > :after_time LIMIT 20;"
                )

    try:
        response = conn_obj.run(query, after_time=after_time)
        return response

    except ProgrammingError as e:
        raise RuntimeError(err_Msg) from e
