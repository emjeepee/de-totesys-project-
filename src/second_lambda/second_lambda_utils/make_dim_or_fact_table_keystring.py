def make_dim_or_fact_table_keystring(table_name: str, timestamp_string: str):
    """
    This table:
        makes a string that is
        a key under which
        another function will
        store either a
        dimension table or a
        fact table in the
        processed bucket.

    Args:
        table_name: the name of
        the table for which
        this function is making
        a timestamp string.
        If table_name is
        'sales_order' then the
        key is for the fact
        table. If table_name is
        anything else then the
        key is for a dimension
        table.

    returns:
        a string that is a key
        under which another
        function will store
        either the fact table
        or a dimension table
        in the processed bucket.

    """

    if table_name == "sales_order":
        table_key = f"fact_{table_name}/{timestamp_string}.parquet"
    else:
        table_key = f"dim_{table_name}/{timestamp_string}.parquet"

    return table_key
