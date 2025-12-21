from .convert_cell_values_main import convert_cell_values_main


def convert_values(updated_rows):
    """
    This function:
        1) receives a list of lists that
         represents updated rows of a
         table in the totesys database.
         A member list represents a
         row. The values in each row
         (list) are field values.

        2) converts the field values in
         member lists in this way:
         i)   json strings -> strings
         ii)  datetime.datetime objects -> strings
         iii) decimal.Decimal objects -> floats

    Args:
        updated_rows: a list of lists that
         represents updated rows of a table
         in the database totesys. Each member
         list represents one row. Each value
         in the row list is a field value.

    Returns:
        A list of lists that is a version of the
         passed in list of lists but with
         i)   json values converted to strings
         ii)  datetime.datetime objects converted to ISO strings
         iii) decimal.Decimal objects converted to floats

    """

    convert_updtd_rows = [
        [
            convert_cell_values_main(value)
            for value in row
        ]
        for row in updated_rows
                         ]

    return convert_updtd_rows
