import os
import boto3

from .conn_to_db import conn_to_db, close_db



def get_env_vars():
    """
    This function:
        1) gets the values of certain 
            environment variables
            for use by all three lambda 
            functions.
        2) creates a dictionary that 
            contains the values of 
            those environment variables 
            and that will act as a 
            lookup table

    Args:
        None

    Returns:
        a dictionary that acts as a lookup 
         table
    """
    
    lookup = {}
    
    # Either way read the other env 
    # vars, make dict with those 
    # values, return the dict:
    tables_list_string = os.environ['AWS_TABLES_LIST'] # 'design, sales_order, ...etc'
    tables_list = [item.strip() for item in tables_list_string.split(",")]

    lookup['tables'] = tables_list # names of all tables of interest (a list): [<table_name_string>, <table_name_string>, ... etc  ]
    lookup['s3_client'] = boto3.client("s3")
    lookup['bucket_name'] = os.environ['AWS_INGEST_BUCKET']     # Name of ingestion S3 bucket, a string  
    lookup['conn'] = conn_to_db(os.environ['OLTP_NAME'])    # A string
    lookup['close_db'] = close_db 


    return lookup





   