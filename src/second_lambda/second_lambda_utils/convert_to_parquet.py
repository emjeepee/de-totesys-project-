import tempfile
import logging

from .write_parquet_to_buffer import write_parquet_to_buffer
from .make_column_defs import make_column_defs
from .make_parts_of_insert_statements import make_parts_of_insert_statements
from .put_pq_table_in_temp_file import put_pq_table_in_temp_file


logger = logging.getLogger(__name__)


def convert_to_parquet(data: list, table_name: str):
    """
    This function:

        1. converts a table in the
        form of a list of
        dictionaries into a table
        in Parquet format in a
        BytesIO buffer.

        2. is called by function
        second_lambda_handler().


    Args:
        data: a list of dictionaries
        representing a table, where
        each dictionary represents a
        row and has key-value pairs
        that represent
        columnname-cellvalue pairs.

        table_name: the name of the
        table

    Returns:
        a BytesIO buffer that contains
        the table in Parquet format.

    """

    # make a string of column names:
    col_defs = make_column_defs(data)
    # col_defs has this form: "some_col_name INT, some_col_name TEXT ..."

    # make parts of the insert
    # statements that DuckDB
    # will use to insert row
    # data from the table into
    # a Parquet file that this
    # function will later make:
    # ph_and_v_list = make_parts_of_insert_statements(data)
    # values_list  = ph_and_v_list[1]
    # placeholders = ph_and_v_list[0]
    placeholders, values_list = make_parts_of_insert_statements(data)

    # Create a temp file with
    # extension .parquet, put the
    # file in tmp_path (which
    # tempfile sets up). The
    # location will be
    # '/tmp/xyz123.parquet',
    # where tempfile generates
    # random character string
    # xyz123. Don't delete the
    # temp file when with block
    # ends:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
        tmp_path = tmp.name

    # use DuckDB to make a
    # Parquet version of the
    # table and save it to
    # the temp path:
    put_pq_table_in_temp_file(table_name,
                              col_defs,
                              values_list,
                              placeholders,
                              tmp_path)

    # Write the Parquet file
    # in the temp location to
    # a BytesIO buffer,
    # permanently delete the
    # temp Parquet file at
    # path tmp_path and
    # return the buffer:
    return write_parquet_to_buffer(tmp_path)
