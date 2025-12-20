from .make_dim_date_python import make_dim_date_python
from .convert_to_parquet import convert_to_parquet


def create_dim_date_Parquet(start_date, timestamp_string: str, num_rows: int):
    """
    This function:
        creates a date
        dimension table as a
        Python list of
        dictionaries and
        converts that list to
        Parquet form.

    Args:
        1) start_date: a
        datetime object for
        current time (minus
        seconds).

        2) timestamp_string:
        holds the date.
        For example "2025-08-14_12-33-27"
        for 12.33pm and
        27 secs, 14Aug2025.

        3) num_rows: the
        number of rows that
        the date dimension
        table will have.
        Equal to the number
        of days the table
        will cover. This is
        also the number of
        days into the past
        from today that the
        date dimension table
        will cover.

    Returns:
        the date dimension
        table as a Parquet
        file in a buffer.

    """

    # Make a date dimension table as a
    # Python list of dictionaries.
    # The date dimensions table has a
    # row for each day. The second arg
    # below is the number of days or
    # rows in the table from start_date:
    dim_date_py = make_dim_date_python(start_date, num_rows)

    # convert_to_parquet() converts the
    # list to Parquet form, puts the
    # Parquet file in a BytessIO buffer
    # returns the buffer:
    dim_date_pq = convert_to_parquet(dim_date_py, "dim_date")

    # Return Parquet file:
    return dim_date_pq
