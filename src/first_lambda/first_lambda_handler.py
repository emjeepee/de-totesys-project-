import os
import logging

from dotenv import load_dotenv

from src.first_lambda.first_lambda_utils.read_table import read_table
from src.first_lambda.first_lambda_utils.get_data_from_db import (
    get_data_from_db
)
from src.first_lambda.first_lambda_utils.write_tables_to_ing_buck import (
    write_tables_to_ing_buck,
)
from src.first_lambda.first_lambda_utils.change_after_time_timestamp import (
    change_after_time_timestamp,
)
from src.first_lambda.first_lambda_utils.get_env_vars import get_env_vars
from src.first_lambda.first_lambda_utils.reorder_list import reorder_list
from src.first_lambda.first_lambda_utils.info_lookup import info_lookup
from src.first_lambda.first_lambda_utils.is_first_run_of_pipeline import (
    is_first_run_of_pipeline,
)
from src.first_lambda.first_lambda_utils.make_updated_tables import (
    make_updated_tables
     )


root_logger = logging.getLogger()

# Create and configure a
# logger that writes to
# a file. set up the
# default settings for
# Python’s logging
# system. Log every
# level of log from
# DEBUG (the lowest
# level) upwards.
# DEBUG (lowest)->INFO->WARNING->ERROR->CRITICAL (highest level)
# The format will be:
# 2025-01-03 12:34:56 [ERROR] root_logger: Failed to write to S3.
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    filemode="a",  # 'a' appends, 'w' would overwrite
)


def first_lambda_handler(event, context):
    """
    This function:
        1) runs every five minutes
           in response to an AWS
           EventBridge trigger.
        2) makes SQL queries to the
            totesys database to get
            updated row data for
            every table that has
            had its row data updated
            in the totesys database.
        3) for each table that has
            had rows updated goes to
            the ingestion bucket and
            gets the previously saved
            table of the same name
            and updates it with the
            new row data.
        4) saves each updated table
             to the ingestion bucket
             under a new key (leaving the
             previous table in place).

    Args:
        1) event: object received from ?????
        2) context: meta data

    Returns:
        None
    """

    # The .env file sets
    # WHAT_ENV to "dev". The
    # Lambda resource block in
    # the child module's
    # main.tf file sets it to
    # "prod":
    if os.environ.get("WHAT_ENV") == "dev":
        load_dotenv(override=True)

    root_logger.info(info_lookup["info_0"])

    if event is None:
        event = {"time": "1900-01-01 00:00:00"}

    # Get values this handler
    # requires and put them
    # in a lookup table:
    lookup = get_env_vars()
    # lookup['tables'] -> [<name string>, <name string>, etc]
    # lookup['s3_client' -> the boto3 s3 client
    # lookup['ing_bucket_name'] -> name of ingestion bucket
    # lookup['proc_bucket_name'] -> name of processed bucket
    # lookup['conn'] -> pg8000.native Connection object
    # lookup['close_db'] -> function close_db()

    # Get the timestamp saved
    # in the S3 ingestion bucket
    # and replace it with one
    # for the current time:
    after_time = change_after_time_timestamp(
        lookup["ing_bucket_name"],  # name of ingestion bucket
        lookup["s3_client"],  # boto3 S3 client object
        "***timestamp***",
        "1900-01-01 00:00:00",
    )

    # Get tables from database
    # totesys that have updated
    # rows (for the first ever
    # run of the pipeline
    # that will be all tables
    # and all rows in them):
    new_table_data = get_data_from_db(
        lookup["tables"],  # list of names of tables of interest
        after_time,
        lookup["conn"],  # pg8000.native Connection object
        read_table,
    )
    # [
    # {'design': [{<updated-row data>}, etc]},
    # {'sales': [{<updated-row data>}, etc]},
    # etc
    # ]
    # where {<updated-row data>} is,
    # eg,
    # {'design_id': 123,
    # 'created_at': 'xxx',
    # 'design_name': 'yyy',
    # etc}

    # Log status:
    root_logger.info(info_lookup["info_1"])

    # If the department table or
    # the address table are in
    # list updated_tables, move
    # them to the front of the
    # list. This ensures that
    # updated versions of those
    # tables are always
    # available for the code in
    # the second lambda handler
    # that creates the dim_staff
    # and dim_counterparty tables:
    data_for_s3 = reorder_list(new_table_data, "address", "department")

    # determine if this is the
    # first ever run of the
    # pipeline:
    is_first_run = is_first_run_of_pipeline(
        lookup["proc_bucket_name"], lookup["s3_client"]
    )

    # if it is the first ever
    # run of the pipeline, save
    # all tables to the
    # ingestion bucket:
    if is_first_run:
        write_tables_to_ing_buck(
            lookup["s3_client"], lookup["ing_bucket_name"], data_for_s3
        )

    # if it is the 2nd-plus
    # run of the pipeline,
    # update the tables and
    # then save them to the
    # ingestion bucket:
    if not is_first_run:
        updated_tables = make_updated_tables(
            data_for_s3, lookup["s3_client"], lookup["ing_bucket_name"]
        )

        write_tables_to_ing_buck(
            lookup["s3_client"], lookup["ing_bucket_name"], updated_tables
        )

    # Log status:
    root_logger.info(info_lookup["info_2"])

    # Close connection to
    # totesys database.
    # lookup['close_db']
    # returns function
    # close_db, which
    # closes the
    # connection to a
    # database:
    lookup["close_db"](lookup["conn"])

    # Log status:
    root_logger.info(info_lookup["info_3"])
