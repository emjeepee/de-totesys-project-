def make_parts_of_insert_statements(data):
    """
    This function:

        1. creates string placeholders,
            a string of ?s separated
            by ', '. The number of ?s
            equals the number of
            columns in the table, eg:
             '?, ?, ?, etc'.

        2. creates values_list, a list
            in which the number of
            members equals the number
            of rows in the table. Each
            member list contains string
            versions of field values,
            eg:
            [
            ["val_1", "val_2", etc],
            ["val_1", "val_2", etc],
            etc
            ]

        3. is called by function
            convert_to_parquet().


    Args:
        data: a list of dictionaries
        that represents a dimension
        table or the fact table.
        Each dictionary represents a
        row and its key-value pairs
        are columnname-fieldvalue
        pairs.

    Returns:
           [placeholders, values_list],
           placeholders: a string of
           comma-separated ?s equal in
           number to the number of
           columns in the table.
           values_list: a list of
           row-value lists, where
           each row value is a string
    """
    # Get the first row from the
    # table, get the column names
    # from the row and make a
    # list of them:
    columns = list(data[0].keys())  # ["col_name_1", "col_name_2", etc]

    # make string of ?s, the number
    # of ?s equalling the number of
    # members of the list columns:
    placeholders = ", ".join("?" for _ in columns)  # '?, ?, ?, etc'

    # Make a list of lists where
    # the number of member lists
    # will equal the number of
    # rows in the table. Each
    # member list contains
    # string versions of the
    # values of a row.

    # replace lines 71-78 with the following single line:
    # values_list = [[str(row[col]) for col in columns] for row in data]


    values_list = []
    for row in data:  # {"col_name_1": "val_1", "col_name_2": "val_2", etc}
        # columns is ["col_name_1", "col_name_2", "col_name_3", etc]
        values = [str(row[col]) for col in columns]
        # values looks like: ["val_1", "val_2", "val_3", etc]
        values_list.append(
            values
        )  # [["val_1", "val_2", etc], ["val_1", "val_2", etc], etc]

    return [placeholders, values_list]
