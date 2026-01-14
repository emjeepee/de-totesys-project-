import json
import logging


from .create_formatted_timestamp import create_formatted_timestamp
from .save_updated_table_to_S3 import save_updated_table_to_S3
from .put_updated_table_in_bucket import put_updated_table_in_bucket

logger = logging.getLogger(__name__)


def write_tables_to_ing_buck(s3_client, bucket_name, data_list):
    """
    This function:
        1) runs at the behest of
            first_lambda_handler()
            if this is the first
            ever run of the piepline.

        2) receives as argument
            data_list, which contains
            all tables and all rows
            of all tables.

        3) saves each table and all
            of its rows in the
            ingestion bucket under a
            key that looks like this:
            'design/<timestamp-here>'

    Args:
        s3_client: a boto3 S3 client.

        bucket_name: a string that
        is the name of the ingestion
        bucket.


        data_list: a list containing
            tables and their rows. This
            list contains member
            dictionaries, each of which
            represents one table.
            data_list looks like this:
            [
            {'design': [{row data>}, {row data>}, etc]},
            {'sales_orders': [{row data>}, {row data>}, etc]},
            etc
            ].
            where keys 'design',
            'sales_orders', etc are the
             names of tables and where
             {<row data>} is, for example:
            {
                'design_id': 123,
                'created_at': 'xxx',
                'design_name': 'yyy',
                etc
            }

    Returns:
        None

    """

    # Make timestamp that 
    # has this form:
    # '2025-06-13_13-13-13'
    timestamp = create_formatted_timestamp()

    # write each table to 
    # the ingestion bucket:
    for table_dict in data_list:  # table_dict is, eg, {'design':
                                            # [{<updated-row data>},
                                            # {<updated-row data>},
                                            # etc]}

        put_updated_table_in_bucket(table_dict,
                                    timestamp,
                                    s3_client,
                                    bucket_name)
    








# OLD CODE:
        # table_name = list(table_dict.keys())[0]  # 'design'

        # # make a list of the 
        # # rows of the table:
        # list_of_rows = table_dict[table_name]

        # # convert the list of
        # # rows to json:
        # json_list_of_rows = json.dumps(list_of_rows)

        # # make key under which
        # # to store the table in
        # # the ingestion bucket:
        # table_key = f"{table_name}/{timestamp}.json" 
        #                                     # 'sales_order/<timestamp>.json'

        # save_updated_table_to_S3(
        #     json_list_of_rows,
        #     s3_client,
        #     table_key,
        #     bucket_name  # name of ingestion bucket
        #                         )
