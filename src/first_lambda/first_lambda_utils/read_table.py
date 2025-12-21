import logging


from .convert_values import convert_values
from .make_row_dicts import make_row_dicts
from .get_updated_rows import get_updated_rows
from .get_column_names import get_column_names
from .make_fake_so_table import make_fake_so_table
from .make_fake_de_table import make_fake_de_table
from .make_fake_ad_table import make_fake_ad_table
from .make_fake_st_table import make_fake_st_table
from .make_fake_cu_table import make_fake_cu_table
from .make_fake_cp_table import make_fake_cp_table
from .make_fake_dp_table import make_fake_dp_table


from pg8000.native import Connection
from os import environ


logger = logging.getLogger(__name__)


def read_table(table_name: str, conn: Connection, after_time: str):
    """
    This function:
         1) makes a dictionary that
            contains those rows of a
            table in the toteSys
            database that have been
            updated. The dictionary's
            sole key is the name of
            the table. The value of
            the key is a list of
            dictionaries, each of
            which represents an
            updated row and contains
            key-value pairs, each of
            which is a
            columnname-fieldvalue
            pair.
         2) makes the dictionary
            above by getting
            i) updated rows from the
            table in the form of a
            list of lists, each member
            list containing only cell
            values for an updated row
            (ie no column names)
            ii) the column names for
            the table in the form of
            a list of lists. Each
            member list contains a
            string for the name of a
            column.
         3) uses the two lists of
            lists in i) and ii) above
            to create the dictionary
            mentioned in 1) above.

    Args:
         1) table_name: the name
          of the table.

         2) conn: an instance of
          a pg8000:native Connection
          object.

         3) after_time: a time stamp
          of the form
          "2025-06-04T08:28:12". If
          the ToteSys database has
          changed a row of the table
          after this time, this function
          considers the row updated.
          On the very first run of
          pipeline the value of
          after_time represents
          1January1900. On subsequent
          runs its value represents
          the time of the previous
          run of the first lambda
          handler.


    Returns:
         a dictionary that looks
         like this: {
                    "sales_order": [
                        {"Name": "xx", "Month": "January",
                            "Value": 123.45, etc}, <-- one updated row
                        {"Name": "yy", "Month": "January",
                            "Value": 223.45, etc}, <-- one updated row
                        {"Name": "zz", "Month": "January",
                            "Value": 323.45, etc}, <-- one updated row
                                        etc
                                         ]
                    }
        where "sales_order" is the
        table name and keys "Name",
        "Month", "Value", etc are
        the table's column names.
        The values of those keys
        are the row-cell values.

    """
    # Get value of env var
    # IS_OLAP_OK and convert
    # to a boolean
    # (IS_OLAP_OK is "False"
    # when the info in the
    # totesys database is
    # no longer usable):
    IS_OLTP_OK = environ["IS_OLTP_OK"]  # "True" or "False"
    # Handle "True", "true", "TRUE", etc:
    IS_OLTP_OK = True if IS_OLTP_OK.lower() == "true" else False

    # The following code no
    # longer runs because
    # maintenance of the data
    # in the totesys database
    # has stopped (Nov25)
    if IS_OLTP_OK:
        # Make a list of the column
        # names of the table in
        # question:
        query_result_2 = get_column_names(
            conn, table_name
        )  # [['staff_id'], ['first_name'], ['last_name'], etc]

        # Get only those rows from
        # the table that contain
        # updated data:
        query_result_1 = get_updated_rows(conn, after_time, table_name)

        # Convert query_result_2 to a
        # list of column-name strings:
        clean_col_names = [
            col[0] for col in query_result_2
        ]  # ['name', 'location', etc]

        # convert field values in the
        # updated rows llike this:
        # datetime.datetime object -> ISO string
        # Decimal value            -> float
        # json                     -> string:
        cleaned_rows = convert_values(query_result_1)

        # Make a dictionary for
        # each updated row where
        # the key-value pairs of
        # each dictionary represent
        # <column-name>: <field-value>:
        row_list_of_dicts = make_row_dicts(clean_col_names, cleaned_rows)
        # [ ...
        #   {"design_id": 6,  "name": "aaa",  "value": 3.14,
        #               "date": '2024-05-01T10:30:00.123456', etc},
        #   {"design_id": 7,  "name": "bbb",  "value": 3.15,
        #               "date": '2024-06-01T10:30:00.123456', etc},
        #   etc ]

    else:
        # This code will run
        # once database totesys
        # stops running:
        so_table = make_fake_so_table()
        de_table = make_fake_de_table()
        add_table = make_fake_ad_table()
        staff_table = make_fake_st_table()
        curr_table = make_fake_cu_table()
        cp_table = make_fake_cp_table()
        dept_table = make_fake_dp_table()

        lookup = {
            "sales_order": so_table,  # [{}, {}, etc]
            "design": de_table,  # [{}, {}, etc]
            "address": add_table,
            "staff": staff_table,
            "currency": curr_table,
            "counterparty": cp_table,
            "department": dept_table,
        }

        row_list_of_dicts = lookup[table_name]

    return {table_name: row_list_of_dicts}
