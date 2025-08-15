import boto3

from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db



def first_lambda_init():
    """
    This function:
        creates a dictionary that is a lookup
         table for values that the first 
         lambda handler will access. 
    
    Args: 
        none.

    Returns:
        A dictionary that is a lookup
         table from which the first lambda 
         handler will access values it 
         requires.
        
    """
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


    lookup = {  
        'tables': tables,  
        's3_client': boto3.client("s3"),
        'bucket_name': "11-ingestion-bucket",    
        'conn': conn_to_db("TOTE_SYS"),
        'close_db': close_db     
            }
    
    return lookup        
    


    

