import os
import logging

from dotenv import load_dotenv

from src.first_lambda.first_lambda_utils.read_table import read_table
from src.first_lambda.first_lambda_utils.get_data_from_db import (
    get_data_from_db)
from src.first_lambda.first_lambda_utils.write_tables_to_ing_buck import (
    write_tables_to_ing_buck)
from src.first_lambda.first_lambda_utils.change_after_time_timestamp import (
    change_after_time_timestamp)
from src.first_lambda.first_lambda_utils.get_env_vars import get_env_vars
from src.first_lambda.first_lambda_utils.reorder_list import reorder_list
from src.first_lambda.first_lambda_utils.info_lookup import info_lookup
from src.first_lambda.first_lambda_utils.is_first_run_of_pipeline import (
    is_first_run_of_pipeline)
from src.first_lambda.first_lambda_utils.make_updated_tables import (
    make_updated_tables)
from src.first_lambda.first_lambda_utils.make_one_updated_table import (
    make_one_updated_table )
from src.first_lambda.first_lambda_utils.put_tables_in_ing_bucket import (
    put_tables_in_ing_bucket)





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
        1) event: object received 
        from event scehduler
        2) context: meta data

    Returns:
        None
    """

    # If code is running in the 
    # development environment
    # WHAT_ENV has value "dev"
    # and code below sets the 
    # environment variables. 
    # For the production 
    # environment WHAT_ENV has 
    # value "prod" and terraform 
    # HCL code sets the 
    # environment variables 
    # for AWS Lambda:
    if os.environ.get("WHAT_ENV") == "dev":
        # Set the environment vars. 
        # Get values from file .env:
        load_dotenv(override=True)


    # log status of this
    # lambda handler
    root_logger.info(info_lookup["info_0"])


    if event is None:
        event = {"time": "1900-01-01 00:00:00"}


    # Make lookup table of 
    # values this handler
    # requires:
    # lookup['tables'] -> [<name string>, <name string>, etc]
    # lookup['s3_client' -> boto3 s3 client
    # lookup['ing_bucket_name'] -> name of ingestion bucket
    # lookup['proc_bucket_name'] -> name of processed bucket
    # lookup['conn'] -> pg8000.native Connection object
    # lookup['close_db'] -> close_db(), closes connection to database totesys
    lookup = get_env_vars()


    # Get the timestamp saved
    # in the S3 ingestion bucket
    # and replace it with one
    # for the current time:
    after_time = change_after_time_timestamp(
        lookup["ing_bucket_name"],  
        lookup["s3_client"],  
        "***timestamp***",
        "1900-01-01 00:00:00",
                                            )

    # Get tables from database
    # totesys that have updated
    # field data. Each table 
    # will contain only updated 
    # rows. For the first ever
    # run of the pipeline
    # new_table_data will contain 
    # all tables and all rows 
    # in them:
    new_table_data = get_data_from_db(
                       lookup["tables"], # list of names of all seven tables 
                       after_time,
                       lookup["conn"],  # pg8000.native Connection object
                       read_table
                                      )
    
    # If database totesys has 
    # updated no rows, stop 
    # the code: 
    if new_table_data  == []:
        return {
            "status": "First lambda handler code skipped",
            "reason": "Database totesys has updated no tables since last run.",
                }

    # new_table_data looks like this:
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
    # list new_table_data, move
    # them to indexes [0] and [1]
    # to ensure that updated 
    # versions of those tables 
    # are always available for 
    # certain code in the second 
    # lambda handler:
    data_for_s3 = reorder_list(new_table_data, "address", "department")


    is_first_run = is_first_run_of_pipeline(
                                            lookup["proc_bucket_name"], 
                                            lookup["s3_client"]
                                           ) # Boolean


    put_tables_in_ing_bucket(is_first_run,
                             lookup["s3_client"],
                             lookup["ing_bucket_name"],
                             data_for_s3
                            )


    # Log status of this
    # lambda handler:
    root_logger.info(info_lookup["info_2"])



    # Close connection to
    # totesys database.
    # lookup['close_db']
    # returns function
    # close_db. lookup["conn"]
    # returns a pg8000.native
    # Connection object:
    lookup["close_db"](lookup["conn"])



    # Log status of this
    # lambda handler:
    root_logger.info(info_lookup["info_3"])
