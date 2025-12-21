# a lookup table for
# error messages that
# first lambda utlilities
# will employ when logging


errors_lookup = {
    # 'err_0' below is
    # no longer used:
    "err_0": """Error caught in get_inbuffer_parquet() while trying to read
    this table in the form of an in-buffer Parquet file from the processed
    bucket table: """,
    "err_1": """Error caught in make_SQL_queries_to_warehouse() while trying
    to connect to the data warehouse.""",
    "err_2": """Error caught in conn_to_db() while trying to connect to the
    data warehouse.""",
    "err_3": """Error caught in close_db() while trying to close the connection
     to the data warehouse.""",
}
