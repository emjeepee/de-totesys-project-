from .preprocess_dim_tables import preprocess_dim_tables


def transform_to_dim_location(address_data: list):
    """
    This function:
        1) transforms the address
            table data as read from
            the ingestion bucket
            into a location
            dimension table.
        2) carries out 1) above by
            changing key
            'address_id' to
            'location_id'

    Args:
        address_data: a list
         of dictionaries that
         represents the address
         table. Each dictionary
         represents a row (and
         its key-value pairs
         represent
         columnName-cellValue
         pairs). This list is
         the unjsonified
         address table that
         the ingestion bucket
         stored.

    Returns:
        A list of dictionaries
        that is the location
        dimension
        table.
    """

    # create the final location
    # dimension table:
    location_dim_table = preprocess_dim_tables(
        address_data, ["created_at", "last_updated"]
    )

    # In each dict in list
    # change key
    # "address_id" to
    # "location_id":
    for dict in location_dim_table:
        dict["location_id"] = dict.pop("address_id")

    return location_dim_table
