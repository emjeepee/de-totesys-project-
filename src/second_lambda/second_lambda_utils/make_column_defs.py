from datetime import date, datetime, time
from decimal import Decimal


def make_column_defs(data):
    """
    convert_to_parquet() calls
    this function, which:

        1. makes a string that
        contains all column
        names and DuckDB types,
        each (column-name +
        type) separated from the
        next by ', '

        2. returns the string.

    Args:
        data: a list of
        dictionaries that
        represents a dimension
        table or the fact table.
        Each dictionary
        represents a row and its
        key-value pairs
        represent
        columnname-cellvalue
        pairs.

    Returns:
        a string that looks like
        this:
        'col1_name TEXT, col2_name INT, ...'

    """

    # Make a lookup table to
    # allow duckdb to
    # determine the type of
    # the data in cells:
    type_map = {
        int: "INTEGER",
        float: "REAL",
        bool: "BOOLEAN",
        str: "TEXT",
        date: "DATE",
        datetime: "TIMESTAMP",
        time: "TIME",
        Decimal: "NUMERIC",
        None: "TEXT",
    }

    # Get the first row from the
    # table:
    first_row = data[0]  # {"col_1_name": 13, "col_2_name": "val_2", etc}

    # make a string that
    # contains all column
    # names joined by ', '
    # and that includes
    # DuckDB types:
    col_defs = ", ".join(  # "col_1_name INT, col_2_name TEXT, etc"
        f"{col} {type_map.get(type(val))}" for col, val in first_row.items()
    )

    return col_defs
