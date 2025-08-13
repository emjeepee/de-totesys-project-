import boto3
from src.first_lambda.first_lambda_utils.write_to_ingestion_bucket import write_to_ingestion_bucket
from src.first_lambda.first_lambda_utils import convert_data, read_table
from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db
from src.first_lambda.first_lambda_utils import get_data_from_db, write_to_s3
from src.first_lambda.first_lambda_utils.write_to_ingestion_bucket import write_to_ingestion_bucket
from src.first_lambda.first_lambda_utils.change_after_time_timestamp import change_after_time_timestamp



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
        "design",           # y
        # "payment",        # n
        "sales_order",      # y
        # "transaction",    # n  
        "counterparty",     # y
        "address",          # y
        "staff",             # y 
        # "purchase_order",  # n
        "department",        #
        "currency",          # y
        # "payment_type",    # n
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


    # Get updated row data from each table
    # in the ToteSys database and write that 
    # data to the bucket.
    # data_for_s3 below looks like this:
    # [ 
    #   {'sales_orders'>: [{<data-from-an-updated-row>}, {<data-from-an-updated-row>}, etc]},
    #   {'design': [{<data-from-an-updated-row>}, {<data-from-an-updated-row>}, etc]},
    #   {'transactions': [{<data-from-an-updated-row>}, {<data-from-an-updated-row>}, etc]},
    #   etc        
    # ] 
    # Each dictionary (eg {'sales_orders'>: []}) contains only those rows that have updated data.

    try:
        data_for_s3 = get_data_from_db(tables, after_time, conn, read_table) # list of dicts
        write_to_s3(data_for_s3, s3_client, write_to_ingestion_bucket, bucket_name, convert_data)
    
    except RuntimeError as e:
        # CloudWatch will log the following error:
        raise RuntimeError from e 

    # Close connection to ToteSys database:
    close_db(conn)



