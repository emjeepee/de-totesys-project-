import boto3
from first_lambda_utils.write_to_ingestion_bucket import write_to_ingestion_bucket
from first_lambda_utils import convert_data, read_table
from first_lambda_utils.conn_to_db import conn_to_db, close_db
from first_lambda_utils import get_data_from_db, write_to_s3
from first_lambda_utils.write_to_ingestion_bucket import write_to_ingestion_bucket
from first_lambda_utils.change_after_time_timestamp import change_after_time_timestamp



def first_lambda_handler(event=None, context=None):
    """
    This function:
        1) runs every five minutes.
        2) gets updated row data for every
         table.
        3) updates the tables.
        4) saves the tables to the S3 ingestion bucket. 

    Args:
        1) event: set to None
        2) context: set to None
    
    Returns:
        None
    """


    if event is None:
        event = {"time": "1900-01-01 00:00:00"}

    s3_client = boto3.client("s3")
    bucket_name = "11-ingestion-bucket"
    # after_time = event["time"]

    # Make a list of names of the 
    # tables in the ToteSys database: 
    tables = [
        "design",
        "payment",
        "sales",
        "transaction",
        "sales_order",
        "counterparty",
        "address",
        "staff",
        "purchase_order",
        "department",
        "currency",
        "payment_type",
    ]

    # Create an instance of a 
    # pg8000.native.Connection
    # object:
    conn = conn_to_db("TOTESYS")


    # Get the timestamp saved in 
    # in the S3 ingestion bucket
    # and replace that timestamp 
    # with one for the current time:
    after_time = change_after_time_timestamp(
        bucket_name, s3_client, "***timestamp***", "1900-01-01 00:00:00"
                                            )


    # Get updated data from each table
    # in the ToteSys database.
    # data_for_s3 below is a list of 
    # jsonified dictionaries, each 
    # dictionary containing a table 
    # name and the updated rows of that 
    # table.
    # Also write data to the bucket:
    try:
        data_for_s3 = get_data_from_db(tables, after_time, conn, read_table)
        write_to_s3(data_for_s3, s3_client, write_to_ingestion_bucket, bucket_name, convert_data)
    
    except RuntimeError as e:
        # CloudWatch will log the following error:
        raise RuntimeError("Something went wrong!") from e 

    # Close connection to ToteSys database:
    close_db(conn)



