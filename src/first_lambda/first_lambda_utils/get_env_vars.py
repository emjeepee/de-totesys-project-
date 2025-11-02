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





    # OLD:
    # ev_lst = os.environ['TF_TABLES_LIST'] # 'design, sales_order, ...etc'
    # tables_list = [item.strip() for item in ev_lst.split(",")]

    # lookup['tables'] = tables_list                             # ['design', 'sales_order', ... etc  ]
    # lookup['s3_client'] = boto3.client("s3")
    # lookup['bucket_name'] = os.environ['TF_INGEST_BUCKET']     # '11-ingestion_bucket'  
    # lookup['conn'] = conn_to_db(os.environ['TF_OLTP_NAME'])    # 'TOTE_SYS'
    # lookup['close_db'] = close_db 


    # check whether env var TF_ENVIRONMENT
    # exists:
    # if "TF_ENVIRONMENT" not in os.environ:
    #     # if no, this is first ever run of 
    #     # the project on local machine, so
    #     # set env vars to values the first
    #     # lambda handler will require, 
    #     # create the dict and return it 
    #     # ():
    #     os.environ["TF_ENVIRONMENT"]    = 'dev'
    #     os.environ['TF_INGEST_BUCKET']  = '11-ingestion_bucket'
    #     os.environ['TF_TABLES_LIST']    = 'design, sales_order, counterparty, address, staff, department, currency'
    #     # os.environ['TF_OLTP_NAME']      = 'TOTE_SYS' # in .zshrc file, as are env vars for connecting to the db.


    # if yes, this is either:
    # 1) the 2nd-plus run of the first 
    # lambda on the local machine and
    # value of "MY_ENV_VAR" is 'dev'.
    # 2) this function is running 
    # because the AWS first lambda has 
    # called it and value of 
    # "MY_ENV_VAR" is 'prod'.
    
    
