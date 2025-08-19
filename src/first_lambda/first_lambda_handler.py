from src.first_lambda.first_lambda_utils.write_to_ingestion_bucket   import write_to_ingestion_bucket
from src.first_lambda.first_lambda_utils.read_table                  import read_table
from src.first_lambda.first_lambda_utils.get_data_from_db            import get_data_from_db
from src.first_lambda.first_lambda_utils.write_to_s3                 import write_to_s3
from src.first_lambda.first_lambda_utils.write_to_ingestion_bucket   import write_to_ingestion_bucket
from src.first_lambda.first_lambda_utils.change_after_time_timestamp import change_after_time_timestamp
from src.first_lambda.first_lambda_utils.first_lambda_init           import first_lambda_init



def first_lambda_handler(event, context):
    """
    This function:
        1) runs every five minutes in response to
            an AWS EventBridge trigger.
        2) makes SQL queries to ToteSys database 
            to get updated row data for every
            table that has had its row data updated 
            in the ToteSys database.
        3) for each table that has had rows updated 
            in the ToteSys database goes to the 
            ingestion bucet and gets the previously 
            saved table of the same name and updates 
            it with the new row data.
        4) saves each updated table to the ingestion
             bucket under a new key (leaving the 
             previous table in place). 

    Args:
        1) event: object received from ????? 
        2) context: ????
    
    Returns:
        None
    """


    if event is None:
        event = {"time": "1900-01-01 00:00:00"}

    # Get values this handler requires:
    lookup = first_lambda_init()

    # Set variables to those values:
    bucket_name = lookup['bucket_name'] # name of ingestion bucket
    tables = lookup['tables'] # list of names of tables of interest
    s3_client = lookup['s3_client'] # boto3 S3 client object
    conn = lookup['conn'] # pg8000.native Connection object
    close_db = lookup['close_db'] # function to close connection to database



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
        write_to_s3(data_for_s3, s3_client, write_to_ingestion_bucket, bucket_name)
    
    except RuntimeError as e:
        # CloudWatch will log the following error:
        raise RuntimeError from e 

    # Close connection to ToteSys database:
    close_db(conn)



