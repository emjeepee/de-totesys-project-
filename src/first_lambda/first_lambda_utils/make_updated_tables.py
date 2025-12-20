from .get_most_recent_table_data import get_most_recent_table_data
from .update_rows_in_table import update_rows_in_table


def make_updated_tables(data_for_s3: list, s3_client, bucket: str):
    """
    This function:
        1) loops through a list of
         tables, each table
         containing only those rows
         that contain updated field
         data

        2) for each table gets
        the corresponding whole
        (and now outdated) table
        from the ingestion bucket
        and updates the appropriate
        rows in it.


    Args:
        data_for_s3: a list of
        dictionaries, where each
        dictionary represents a
        table from database
        totesys that has had rows
        updated. The list looks
        like this:
            [
            {'design': [{<updated-row data>}, etc]},
            {'sales': [{<updated-row data>}, etc]},
            etc
            ]
            where {<updated-row data>} is,
            eg, {'design_id': 123,
                  'created_at': 'xxx',
                  'design_name': 'yyy',
                  etc}
            and the number of
            {<updated-row data>}
            dictionaries is equal
            to the number of rows that
            contain updated field data.

        s3_client: a boto3 S3 client.

        bucket: name of the ingestion
        bucket


        Returns:
          a list of dictionaries
          like this:
          [
          {'design': [{row}, {row}, {row}, etc]},
          {'sales_order': [{row}, {row}, {row}, etc]},
          etc
          ]
          where each dictionary is a
          whole table the appropriate
          rows of which now contain
          updated field data.
    """

    updated_tables = []

    for member in data_for_s3:
        table_name = list(member.keys())[0]  # 'design'

        # From the ingestion bucket
        # get the object that holds
        # the most recently updated
        # table of name table_name.
        latest_table = get_most_recent_table_data(
            table_name, s3_client, bucket
                                                 )  # a list of dictionaries.

        # Insert the updated rows into the
        # retrieved whole table, replacing
        # the outdated ones:
        updated_table = update_rows_in_table(  # [{<updated row>},
                                            # {<updated row>}, etc]
                                            member[table_name],
                                            latest_table,
                                            table_name
                                            )

        updated_tables.append({table_name: updated_table})

    return updated_tables
