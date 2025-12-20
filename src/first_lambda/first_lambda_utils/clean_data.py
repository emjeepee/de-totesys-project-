import copy

from .change_vals_to_strings import change_vals_to_strings


def clean_data(table, table_dict):
    """
    This function:
        1. receives a table from the
        totesys database. the table
        will include rows whose
        field data the totesys
        database has updated since
        a the last run of this
        pipeline.

        2. makes field data in
        the rows json safe (to
        allow a later function
        to put the table into
        the ingestion bucket in
        json form). This means
        changing
        datetime.datetime objects
        to iso strings
        and
        decimal.Decimal objects
        to strings.


    args:
        table: the name of a table.

        table_dict: a dictionary
        whose sole key is the name
        of a table. The value of
        the key is a list of
        dictionaries, each
        dictionary representing a
        row of the table. Each
        row contains field data
        that database totesys has
        updated since the last
        run of this pipeline.



    returns:
        a dictionary with sole key
        is the name of a table and
        whose value is a list of
        dictionaries, each
        dictionary representing a
        row of the table if that row
        contains updated field data.
        The field data in the row is
        now clean (ie of the correct
        type).


    """

    # change
    # datetime.datetime objs ->  iso strings
    # decimal.Decimal obj -> strings:

    lst_deep_copy = copy.deepcopy(table_dict[table])  # [{...}, {...}, etc]
    for dct in lst_deep_copy:
        for ky, val in dct.items():
            change_vals_to_strings(ky, val, dct)

    return {table: lst_deep_copy}
