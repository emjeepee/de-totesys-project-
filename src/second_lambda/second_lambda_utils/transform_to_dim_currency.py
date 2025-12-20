from .preprocess_dim_tables import preprocess_dim_tables
from .make_curr_obj import make_curr_obj


def transform_to_dim_currency(currency_data):
    """
    This function:
        1) transforms the currency
            table data that came
            from the ingestion bucket
            into the currency
            dimension table. In doing
            so it gets rid of
            unnecessary key-value
            pairs and adds one
            key-value pair:
            'currency_name': <name-of-currency>.

    Args:
        currency_data: a list of d
        ictionaries that came from
        the ingestion bucket and
        represents the currency
        table. Each dictionary
        represents a row.

    Returns:
        A list of dictionaries that
        is the currency dimension
        table.
    """

    # Make a preprocessed dimension
    # currency table (pp_curr_dim_table)
    # by removing unwanted columns and
    # their cell values:
    pp_curr_dim_table = preprocess_dim_tables(
        currency_data, ["created_at", "last_updated"]
    )

    for row_dict in pp_curr_dim_table:
        # row dict look like: # {'currency_id': 3, 'currency_code': 'EUR'}
        curr_obj = make_curr_obj(row_dict)
        curr_name = curr_obj.name
        row_dict["currency_name"] = curr_name

        # pp_curr_dim_table is
        # now the finished currency
        # dimension table.
        # Its columns are: 'currency_id', 'currency_code', 'currency_name'
        #
        #
        #
        # Return it:
    return pp_curr_dim_table

# Typical currency table cols and typical values
# [['currency_id'], ['currency_code'], ['created_at'], ['last_updated']]
# [[3, 'EUR', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
# datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]
