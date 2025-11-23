import json
import boto3
import logging

from datetime import datetime

from second_lambda_utils.create_dim_date_Parquet  import create_dim_date_Parquet
from second_lambda_utils.read_from_s3             import read_from_s3
from second_lambda_utils.convert_to_parquet       import convert_to_parquet
from second_lambda_utils.upload_to_s3             import upload_to_s3
from second_lambda_utils.second_lambda_init       import second_lambda_init
from second_lambda_utils.make_dim_or_fact_table   import make_dim_or_fact_table
from second_lambda_utils.is_first_run_of_pipeline import is_first_run_of_pipeline
from second_lambda_utils.should_make_dim_date     import should_make_dim_date






logger = logging.getLogger()

def second_lambda_handler(event, context):
    """
    This function:
        1) Either:
            i)   Reads an object from the ingestion bucket
                 and converts it into a dimension table
                 or fact table, converts that table into
                 a Parquet file and writes the Parquet
                 file to the processed S3 bucket.
        
            ii)  Creates a date dimension table if this 
                 is the first ever run of the pipeline, 
                 converts that table into a Parquet 
                 file and writes that file to the 
                 processed S3 bucket.
        
        2) carries out 1)i) and 1)ii) in 
            in reponse to an event from 
            the S3 ingestion bucket when 
            the first lambda function 
            has saved a table in that 
            bucket. The ingestion bucket 
            stores each table as a 
            jsonified list of 
            dictionaries, where each 
            dictionary represents a row 
            of the table. 
        3) gets the name of the S3 
            ingestion bucket from the 
            passed-in event and the name 
            of the key under which the 
            first lambda stored the 
            table in that bucket.
        3) gets the table stored under 
            the key, converts it to a 
            dimension table or fact 
            table (in the form of a list
            of dictionaries), converts 
            that to a Parquet file and 
            stores the Parquet file in 
            a BytesIO buffer in the 
            processed S3 bucket under 
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
        handler stores a table in 
        the ingestion bucket.

    Returns:
        None                    
    """

    # Get lookup table that contains 
    # values this handler requires:
    lookup = second_lambda_init(event, boto3.client("s3"), datetime.now(), datetime(2024, 1, 1), 2557)
    
    # Set vars to values in lookup table: 
    s3_client = lookup['s3_client'] # boto3 S3 client object,
    ingestion_bucket = lookup['ingestion_bucket'] # name of bucket,
    object_key = lookup['object_key'] # bucket stores object under this key
    timestamp_string = lookup['timestamp_string'] # a timestamp string
    table_name = lookup['table_name'] # name of table
    proc_bucket = lookup['proc_bucket'] # name of processed bucket
    start_date = lookup['start_date'] # a datetime object for 1 Jan 2024
    num_rows = lookup['num_rows'] # an int. number of rows in dimensions table


    err_msg = 'Error in second_lambda_handler()'
    

    # If the table just put in the 
    # ingestion bucket is "department"
    # go no further (because there 
    # is no need to create a department
    # dimension table):
    if table_name == "department":
        # simply stop this lambda 
        # handler:
        return {
            "status": "skipped",
            "reason": "table_name = department"
               }


    # Get the jsonified python 
    # list that is the single 
    # table that this lambda 
    # handler has just been 
    # notified about:
    
    try:
        table_json = read_from_s3(s3_client, ingestion_bucket, object_key) # jsonified [{<row data>}, {<row data>}, etc]
                                                                           # where {<row data>} is, eg,
                                                                           # {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}        
    except Exception:
        logger.error(err_msg)
        # Need the raise below
        # otherwise when read_from_s3 
        # raises an exception and 
        # the error message is logged 
        # the line below 
        # (json.loads(table_json))
        # will run too, but table_json
        # will not have been set! 
        # Could also use a return:
        raise
    
    
    # make a list version of 
    # the table:
    table_python = json.loads(table_json) # [{<row data>}, {<row data>}, etc]
                                          # where {<row data>} is, eg,
                                          # {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}        
    print(f"MY_INFO >>>>> In second lambda handler. table_python is {table_python}")        

    try:
    # If this is the first 
    # ever run of the pipeline 
    # (ie if the processed 
    # bucket is empty) make a 
    # date dimension table in 
    # Parquet form and save it 
    # in the processed bucket: 
        print(f"MY_INFO >>>>> In second lambda handler. About to call funcion should_make_dim_date()")
        should_make_dim_date(is_first_run_of_pipeline, 
                             create_dim_date_Parquet, 
                             upload_to_s3, 
                             start_date, 
                             timestamp_string, 
                             num_rows, 
                             proc_bucket, 
                             s3_client)
        
    except Exception:
        logger.error(err_msg)
        raise
                    
        

    try:
        # Make the fact table 
        # or a dimension table 
        # that looks like this:
        # [{<row data>}, {<row data>}, etc] 
        # where {<row data>} is, eg,
        # {
        # 'design_id': 123, 
        # 'abcdef': 'xxx', 
        # 'design_name': 'yyy', 
        # etc
        # }     
        print(f"MY_INFO >>>>> In second lambda handler. About to call funcion make_dim_or_fact_table()")    
        dim_or_fact_table = make_dim_or_fact_table(table_name, 
                                                   table_python, 
                                                   s3_client, 
                                                   ingestion_bucket)

    except Exception:
        logger.error(err_msg)
        # following line needed 
        # to ensure code stops
        # running (could also 
        # have used a return):
        raise 


    # Use Duckdb to convert the 
    # dim/fact table to a 
    # Parquet file in a buffer: 
    # print(f"MY_INFO >>>>> In second lambda handler. About to call function convert_to_parquet()")    
    pq_file = convert_to_parquet(dim_or_fact_table, table_name) # a buffer

    # Create the key (a string) under 
    # which to save the dim/fact table 
    # in the processed bucket:
    table_key = f"fact_{table_name}/{timestamp_string}.parquet" if table_name == "sales_order" else f"dim_{table_name}/{timestamp_string}.parquet"


    try:
        # Write the Parquet file in 
        # the processed bucket:
        # print(f"MY_INFO >>>>> In second lambda handler. About to call function upload_to_s3()")    
        upload_to_s3(s3_client, proc_bucket, table_key, pq_file)
    except Exception:
        logger.error(err_msg)
        raise
