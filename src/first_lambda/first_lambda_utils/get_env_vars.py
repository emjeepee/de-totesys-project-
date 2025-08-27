import os
import boto3

from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db





def set_env_vars_for_all_lamb_fns():
    """
    This function:
        1) checks whether environment variable 
            TF_ENVIRONMENT exists. If it does,
            this function gets from other 
            environment variables certain 
            values that the first lambda 
            function requires. The 
            non-existence of TF_ENVIRONMENT 
            means that this function is 
            running in the development 
            environment. This function then 
            sets the keys of a dictionary to 
            the values held in other 
            environment variables.
        2) sets the environment variables
            for use by all three lambda 
            functions.
        3) includes the setting of 
            environment variable 
            TF_ENVIRONMENT, whose value 
            determines whether the 
            environment is development or 
            production.
        4) gets called by the first 
            lambda function. 
        5) returns the value of             
            TF_ENVIRONMENT
            when this 
            happens Terraform will have 
            set the value of environment
            variable TF_ENVIRONMENT to
            'prod'. This funcion will 
            then return, doing nothing.
            In development


    Args:
        None

    Returns:
        None        
   
    """

def get_env_vars():
    
    lookup = {}
    
    # check whether env var TF_ENVIRONMENT
    # exists:
    if "TF_ENVIRONMENT" not in os.environ:
        # if no, this is first ever run of 
        # the project on local machine, so
        # set env vars to values the first
        # lambda handler will require, 
        # create the dict and return it 
        # ():
        os.environ["TF_ENVIRONMENT"]    = 'dev'
        os.environ['TF_INGEST_BUCKET']  = '11-ingestion_bucket'
        os.environ['TF_TABLES_LIST']    = 'design, sales_order, counterparty, address, staff, department, currency'
        # os.environ['TF_OLTP_NAME']      = 'TOTE_SYS' # in .zshrc file, as are env vars for connecting to the db.


    # if yes, this is either:
    # 1) the 2nd-plus run of the first 
    # lambda on the local machine and
    # value of "MY_ENV_VAR" is 'dev'.
    # 2) this function is running 
    # because the AWS first lambda has 
    # called it and value of 
    # "MY_ENV_VAR" is 'prod'.
    
    
    # Either way read the other env 
    # vars, make dict with those 
    # values, return the dict:
    ev_lst = os.environ['TF_TABLES_LIST'] # 'design, sales_order, ...etc'
    tables_list = [item.strip() for item in ev_lst.split(",")]

    lookup['tables'] = tables_list                             # ['design', 'sales_order', ... etc  ]
    lookup['s3_client'] = boto3.client("s3")
    lookup['bucket_name'] = os.environ['TF_INGEST_BUCKET']     # '11-ingestion_bucket'  
    lookup['conn'] = conn_to_db(os.environ['TF_OLTP_NAME'])    # 'TOTE_SYS'
    lookup['close_db'] = close_db 


    return lookup

