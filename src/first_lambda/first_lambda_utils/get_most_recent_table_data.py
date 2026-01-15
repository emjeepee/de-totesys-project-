import boto3
import logging


from botocore.exceptions import ClientError

from .get_latest_table import get_latest_table
from .errors_lookup import errors_lookup


logger = logging.getLogger(__name__)


def get_most_recent_table_data(
                                file_location: str, 
                                S3_client: boto3.client, 
                                bucket_name: str
                              ):
    
    """
    This function:
        1) polls a boto3 S3 client for 
            a response to a request for 
            a list of objects in 
            the ingestion bucket that 
            the bucket stores under keys 
            that begin '<file_location>'
            (the ingestion bucket stores
            each table under a key that
            looks like this:
            <file_location>/<timestamp>.json.
            The passed-in file_location
            is the name of the table).
            
        2) passes the response to function 
            get_latest_table(), which 
            ues the response to get the 
            most recent table of name 
            file_location.

        3) gets called by make_one_updated_table()            


    Args:
        1) file_location: part of a key under
            which the ingestion bucket stores
            jsonified lists of dictionaries.
            Each list represents a table and
            each dictionary in the list
            represents a row of the table.
            This arg is also the name of a
            table, eg 'design'.

        2) S3_client: the boto3 S3 bucket 
            client

        3) bucket_name: the bucket name

    Returns:
        A python list of dictionaries that
         represents the most recently saved
         table of name file_location.


    """

    try:
        response = S3_client.list_objects_v2(Bucket=bucket_name,
                                             Prefix=file_location)
        
        latest_tbl = get_latest_table(response, 
                                      S3_client, 
                                      bucket_name) # list
        
        return latest_tbl


    except ClientError:
        # log the error and
        # stop the code:
        logger.exception(errors_lookup["err_4"] + f"{file_location}")
        raise

