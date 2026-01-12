import copy


def insert_updated_rows( num_of_rows: int, 
                         whole_table: list, 
                         updated_rows: list, 
                         id_col_name: str ):
    """
    This function:
        takes updated rows from a
        list and inserts them into
        a list that is a whole
        table in the appropriate
        places.

    Args:
        num_of_rows: the number of
        rows in the whole (and
        outdated) table.

        whole_table: a whole table,
        some of whose rows are
        outdated. This is in the
        form of a list of
        dictionaries, each
        dictionary representing a
        row of the table.

        updated_rows: a list of
        dictionaries, each
        dictionary representing a
        row that contains updated
        field data.

        id_col_name: the name of
        the column that is the id
        of the table (and the
        primary key), eg
        'design_id'.

    returns:
        The whole table but with
        updated rows.

    """

    wt_copy = copy.deepcopy(whole_table)

    for dictn in updated_rows:
        for i in range(num_of_rows):  # 0,1,2 when num_of_rows is 3
            if whole_table[i][id_col_name] == dictn[id_col_name]:
                wt_copy[i] = dictn
                break

    return wt_copy
