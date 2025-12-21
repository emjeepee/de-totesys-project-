import duckdb

from .read_parquet_from_buffer import read_parquet_from_buffer
from .make_list_of_query_strings import make_list_of_query_strings
from .get_columns_and_rows import get_columns_and_rows


def make_insert_queries_from_parq(parquet_buffer, table_name):
    """
    This function:
        generates a list of
        PostgreSQL INSERT queries.
        Each query relates to a
        row of data in a table
        passed into this function
        in the form of a Parquet
        file in a BytesIO buffer.

    Args:
        parquet_buffer: a BytesIO
        buffer containing a
        dimension or fact table
        in Parquet form.

        table_name: Name of the
        table in the data warehouse
        that this function must
        insert data into, eg
        'dim_design' or
        'fact_sales_orders'.

    Returns:
        list: a list of SQL INSERT
        query strings.

    """

    # make an in-memory
    # duckdb database:
    conn = duckdb.connect(":memory:")

    # get a DuckDB query
    # result object from
    # the buffer:
    DDB_q_result_obj = read_parquet_from_buffer(parquet_buffer, conn)

    # Make a list contaning
    # 1. a string made up of
    # all column names
    # 2. a list containing
    # tuples each of which
    # contains row values:
    rows_cols = get_columns_and_rows(DDB_q_result_obj)  # [column_str, rows]

    # generate INSERT query
    # strings and put them in
    # a list:
    queries = make_list_of_query_strings(
                                         rows_cols[1],
                                         table_name,
                                         rows_cols[0]
                                        )

    # close duckdb in-memory
    # database:
    conn.close()

    return queries
