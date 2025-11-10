from first_lambda_utils.write_to_ingestion_bucket   import write_to_ingestion_bucket
from first_lambda_utils.read_table                  import read_table
from first_lambda_utils.get_data_from_db            import get_data_from_db
from first_lambda_utils.write_to_s3                 import write_to_s3
from first_lambda_utils.write_to_ingestion_bucket   import write_to_ingestion_bucket
from first_lambda_utils.change_after_time_timestamp import change_after_time_timestamp
from first_lambda_utils.get_env_vars                import get_env_vars



import logging


root_logger = logging.getLogger()

# Logging config.
# Create and configure a logger 
# that writes to a file:
logging.basicConfig(
    level=logging.DEBUG,                                         # Log level (includes INFO, WARNING, ERROR)
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",  # Log format
    filemode="a"                                                 # 'a' appends; 'w' would overwrite
                   )



def first_lambda_handler(event, context):
    """
    This function:
        1) runs every five minutes in response to
            an AWS EventBridge trigger.
        2) makes SQL queries to totesys database 
            to get updated row data for every
            table that has had its row data updated 
            in the totesys database.
        3) for each table that has had rows updated 
            in the totesys database goes to the 
            ingestion bucket and gets the previously 
            saved table of the same name and updates 
            it with the new row data.
        4) saves each updated table to the ingestion
             bucket under a new key (leaving the 
             previous table in place). 

    Args:
        1) event: object received from ????? 
        2) context: meta data
    
    Returns:
        None
    """
    root_logger.info("First Lambda function starting anew.\n\n")

    if event is None:
        event = {"time": "1900-01-01 00:00:00"}

    # Get values this handler requires:
    lookup = get_env_vars()

    # Set variables to those values:
    bucket_name = lookup['bucket_name'] # name of ingestion bucket
    tables = lookup['tables'] # list of names of tables of interest
    s3_client = lookup['s3_client'] # boto3 S3 client object
    conn = lookup['conn'] # pg8000.native Connection object
    close_db = lookup['close_db'] # function to close connection to database



    
    try:
        # Get the timestamp saved in 
        # in the S3 ingestion bucket
        # and replace that timestamp 
        # with one for the current time:
        after_time = change_after_time_timestamp(
        bucket_name, s3_client, "***timestamp***", "1900-01-01 00:00:00"
                                            )
    except Exception:
        # The following line automatically logs 
        # the full traceback of the most recent 
        # exception.
        root_logger.exception("Error caught in first Lambda function while trying to run change_after_time_timestamp()\n\n")      


    try: 
        # Find only those tables in the ToteSys 
        # database that have updated rows.
        data_for_s3 = get_data_from_db(tables, after_time, conn, read_table) 
            # [{'design': [{<updated-row data>}, etc]}, {'sales': [{<updated-row data>}, etc]}, etc].
            # where {<updated-row data>} is eg {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}
    except Exception: 
        root_logger.exception("Error caught in first_lambda_handler() while trying to run get_data_from_db()\n\n")    


    try:
        # write updated row data from each
        # table to the ingestion bucket: 
        write_to_s3(data_for_s3, s3_client, write_to_ingestion_bucket, bucket_name)
    except Exception: 
        root_logger.exception("Error caught in first_lambda_handler() while trying to run write_to_s3()\n\n")    
    

    # Close connection to ToteSys database:
    close_db(conn)

    



