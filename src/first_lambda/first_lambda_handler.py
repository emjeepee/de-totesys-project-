from first_lambda_utils.write_to_ingestion_bucket   import write_to_ingestion_bucket
from first_lambda_utils.read_table                  import read_table
from first_lambda_utils.get_data_from_db            import get_data_from_db
from first_lambda_utils.write_to_s3                 import write_to_s3
from first_lambda_utils.write_to_ingestion_bucket   import write_to_ingestion_bucket
from first_lambda_utils.change_after_time_timestamp import change_after_time_timestamp
from first_lambda_utils.get_env_vars                import get_env_vars
from first_lambda_utils.reorder_list                import reorder_list
from first_lambda_utils.errors_lookup               import errors_lookup
from first_lambda_utils.info_lookup                 import info_lookup

import logging


root_logger = logging.getLogger()

# Create and configure a logger 
# that writes to a file:
logging.basicConfig(
    level=logging.DEBUG,                                         # Log level (includes INFO, WARNING, ERROR)
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",  # Log format
    filemode="a"                                                 # 'a' appends. 'w' would overwrite
                   )



def first_lambda_handler(event, context):
    """
    This function:
        1) runs every five minutes 
           in response to an AWS 
           EventBridge trigger.
        2) makes SQL queries to the
            totesys database to get 
            updated row data for 
            every table that has 
            had its row data updated 
            in the totesys database.
        3) for each table that has 
            had rows updated goes to 
            the ingestion bucket and 
            gets the previously saved 
            table of the same name 
            and updates it with the 
            new row data.
        4) saves each updated table 
             to the ingestion bucket 
             under a new key (leaving the 
             previous table in place). 

    Args:
        1) event: object received from ????? 
        2) context: meta data
    
    Returns:
        None
    """
    root_logger.info(info_lookup['info_0'])

    if event is None:
        event = {"time": "1900-01-01 00:00:00"}

    # Get values this handler 
    # requires and put them 
    # in a lookup table:
    lookup = get_env_vars()
    

    # Get the timestamp saved 
    # in the S3 ingestion bucket
    # and replace it with one 
    # for the current time:
    after_time = change_after_time_timestamp(
                    lookup['bucket_name'], # name of ingestion bucket
                    lookup['s3_client'], # boto3 S3 client object
                    "***timestamp***", 
                    "1900-01-01 00:00:00"
                                            )



    # Find only those tables 
    # in the totesys database 
    # that have updated rows:
    updated_tables = get_data_from_db(
                lookup['tables'], # list of names of tables of interest 
                after_time, 
                lookup['conn'], # pg8000.native Connection object 
                read_table) 
            # [
            # {'design': [{<updated-row data>}, etc]}, 
            # {'sales': [{<updated-row data>}, etc]}, 
            # etc
            # ]
            # where {<updated-row data>} is, eg, {'design_id': 123, 'created_at': 'xxx', 'design_name': 'yyy', etc}
    
    # Log status:
    root_logger.info(info_lookup['info_1'])

    # If the department table or 
    # the address table are in
    # list updated_tables, move 
    # them to the front of the 
    # list. This ensures that 
    # updated versions of those
    # tables are always 
    # available for the code in 
    # the second lambda handler 
    # that creates the dim_staff 
    # and dim_counterparty tables:
    data_for_s3 = reorder_list(
                updated_tables, 
                "address", 
                "department")



    # write updated row data 
    # from each table to the 
    # ingestion bucket: 
    write_to_s3(data_for_s3, 
                lookup['s3_client'], # boto3 S3 client object, 
                write_to_ingestion_bucket, 
                lookup['bucket_name'])
  
    # Log status:
    root_logger.info(info_lookup['info_2'])



    # Close connection to 
    # totesys database.
    # lookup['close_db'] 
    # returns function 
    # conn_to_db(), which
    # closes the 
    # connection to a
    # database:
    lookup['close_db'](lookup['conn'])

    # Log status:
    root_logger.info(info_lookup['info_3'])

    



