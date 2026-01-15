import json
import boto3
import logging

from datetime import datetime

from src.second_lambda.second_lambda_utils.create_dim_date_Parquet import (
    create_dim_date_Parquet,
)
from src.second_lambda.second_lambda_utils.read_from_s3 import read_from_s3
from src.second_lambda.second_lambda_utils.convert_to_parquet import (
    convert_to_parquet)
from src.second_lambda.second_lambda_utils.upload_to_s3 import upload_to_s3
from src.second_lambda.second_lambda_utils.second_lambda_init import (
    second_lambda_init)
from src.second_lambda.second_lambda_utils.make_dim_or_fact_table import (
    make_dim_or_fact_table)
from src.second_lambda.second_lambda_utils.is_first_run_of_pipeline import (
    is_first_run_of_pipeline)
from src.second_lambda.second_lambda_utils.info_lookup import info_lookup
from src.second_lambda.second_lambda_utils.create_formatted_timestamp import (
    create_formatted_timestamp)
from src.second_lambda.second_lambda_utils.make_dim_or_fact_tbl_keystr import(
    make_dim_or_fact_tbl_keystr)


root_logger = logging.getLogger()

# Create and configure a logger
# that writes to a file:
logging.basicConfig(
    level=logging.DEBUG,  # Log level (includes INFO, WARNING, ERROR)
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",  # Log format
    filemode="a",  # 'a' appends. 'w' would overwrite
                    )


def second_lambda_handler(event, context):
    """
    This function:
        1) responds to an event that 
            AWS S3 sends to it after 
            the first lambda handler 
            has put a table in the 
            ingestion bucket.
        
        2) either:
            i)   Reads an object from the
                 ingestion bucket and
                 converts it into a
                 dimension table or fact
                 table, converts that
                 table into a Parquet
                 file, puts the Parquet 
                 file into a BytesIO buffer 
                 and writes the buffer
                 to the processed S3 bucket.

            ii)  Creates a date
                 dimension table if this
                 is the first ever run of
                 the pipeline, converts
                 that table into a Parquet
                 file, puts the Parquet 
                 file into a BytesIO 
                 buffer and puts the buffer 
                 in the processed S3 bucket.

        3) gets the table stored under
            the key obtained from the
            event, converts it to a
            dimension table or fact
            table (in the form of a list
            of dictionaries), converts
            that to a Parquet file and
            stores the Parquet file in
            a BytesIO buffer in the
            processed bucket under
            key
            f"{timestamp}/fact_sales_orders.parquet"
              or
            f"{timestamp}/dim_{table_name}.parquet",
            where timestamp looks like this:
            "2025-08-14_12-33-27".

    Args:
        event: the event object
        sent to this lambda
        handler by AWS EventBridge
        when the first lambda
        handler puts a table in
        the ingestion bucket.

    Returns:
        None
    """

    # log status:
    root_logger.info(info_lookup["info_0"])

    # Make lookup table 
    # that contains values 
    # this handler requires:
    lookup = second_lambda_init(
                            event,
                            boto3.client("s3"),
                            datetime.now(),
                            datetime(2024, 1, 1),
                            2557
                               )

    # If the table just put in the
    # ingestion bucket is 
    # "department" simply end the code 
    # because there is no need to 
    # create a department dimension 
    # table:
    if lookup["table_name"] == "department":
        return {
            "status": "Second lambda handler code skipped",
            "reason": "The table is 'department' ",
                }


    # get the table just 
    # put in the ingestion 
    # bucket:
    table_json = read_from_s3(
        lookup["s3_client"], 
        lookup["ingestion_bucket"],
        lookup["object_key"],
                             )

    # convert the table into
    # a list:
    table_python = json.loads(table_json) # [{<row data>}, {<row data>}, etc]
    # where {<row data>} is, eg,
    # {
    # 'design_id': 123,
    # 'created_at': 'xxx',
    # 'design_name': 'yyy',
    # etc
    # }


    is_first_run = is_first_run_of_pipeline(lookup["proc_bucket"],
                                            lookup["s3_client"])

    if is_first_run:
        # make Parquet table,
        # put it in a buffer
        # and return the buffer:
        pq_table_in_buff = create_dim_date_Parquet(
            lookup["start_date"],
            lookup["timestamp_string"],
            lookup["num_rows"]
                                                   )


        ts = create_formatted_timestamp()

        dim_date_key = f"dim_date/{ts}.parquet"

        # write the table to 
        # the processed bucket:
        upload_to_s3(
            lookup["s3_client"],
            lookup["proc_bucket"],
            dim_date_key,
            pq_table_in_buff
                    )

    # Make the fact table or
    # a dimension table that
    # looks like this:
    # [{<row data>},
    # {<row data>},
    # etc]
    # where {<row data>} is, eg,
    # {
    # 'design_id': 123,
    # 'abcdef': 'xxx',
    # 'design_name': 'yyy',
    # etc
    # }
    dim_or_fact_table = make_dim_or_fact_table(
        lookup["table_name"],  # name of table
        table_python,
        lookup["s3_client"],
        lookup["ingestion_bucket"],
    )

    # Convert the dim/fact
    # table into a Parquet
    # file in a buffer:
    pq_dim_or_fact = convert_to_parquet(dim_or_fact_table, 
                                        lookup["table_name"]
                                        )  # a buffer

    # Create the key string
    # under which to save
    # the dim/fact table in
    # the processed bucket:
    key = make_dim_or_fact_tbl_keystr(
                                      lookup["table_name"],
                                      lookup["timestamp_string"]
                                     )

    # Put the dim/fact table
    # Parquet file in the
    # processed bucket:
    upload_to_s3(lookup["s3_client"],
                 lookup["proc_bucket"],
                 key, 
                 pq_dim_or_fact
                 )

    # log status:
    root_logger.info(info_lookup["info_1"])