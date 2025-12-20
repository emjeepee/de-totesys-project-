import os
import boto3

from .conn_to_db import conn_to_db, close_db


def get_env_vars():
    """
    This function:
        1) gets the values of
            environment variables
            so that the first lambda
            function can employ
            them.
        2) puts those values in a
            dictionary that will act
            as a lookup table for
            use in
            first_lambda_handler().

    Args:
        None

    Returns:
        a dictionary that acts as
        a lookup table.
    """

    lookup = {}

    # Either way read the other env
    # vars, make dict with those
    # values, return the dict:
    tables_list_string = os.environ["AWS_TABLES_LIST"]  # 'design, sales_order,
    # ...etc'

    lookup["tables"] = [
        item.strip() for item in tables_list_string.split(",")
    ]  # names of all tables
    # of interest:
    # [<name string>, <name string>, etc]

    lookup["s3_client"] = boto3.client("s3")

    lookup["ing_bucket_name"] = os.environ["AWS_INGEST_BUCKET"]  # Name of
    # S3 ingestion bucket, a string.

    lookup["proc_bucket_name"] = os.environ["AWS_PROCESS_BUCKET"]  # Name of
    # S3 ingestion bucket, a string.

    lookup["conn"] = conn_to_db(os.environ["OLTP_NAME"])  # pg8000.native
    # Connection object
    # with access to
    # OLTP database
    # totesys

    lookup["close_db"] = close_db  # function to close connection
    # to OLTP database totesys

    return lookup
