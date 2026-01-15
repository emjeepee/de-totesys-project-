import boto3
import logging

from .errors_lookup import errors_lookup
from .retrieve_latest_table import retrieve_latest_table
from .make_table_name_and_key import make_table_name_and_key


logger = logging.getLogger(__name__)


def get_latest_table(resp_dict, 
                     S3_client: boto3.client, 
                     bucket_name: str):
    """
    This function:
        1) has the job of getting the 
            latest version of table 
            table_name from the 
            ingestion bucket

        2) looks at the passed-in resp_dict,
            which is a response to a call 
            to a boto3 S3_client to list 
            all objects that have a 
            particular table name in their 
            keys.
            
        3) gets a list of those keys from 
            that response.

        4) sorts the list in time order

        5) gets the latest key

        6) passes that key to function
            retrieve_latest_table() to 
            get the latest table (which 
            the ingestion bucket has 
            stored under that key)

        7) returns the table

        8) gets called by
            get_most_recent_table_data().

            

    Args:
        1) resp_dict: a dictionary that boto3
            method S3_client.list_objects_v2()
            returns. Looks like this:
            {
                'IsTruncated': True|False,
                'Contents': [
                        {
                    'Key': 'string',  # objects of interest are stored
                                      # under this key, which looks
                                      # like this:
                                      # '<table_name_here>/<timestamp_here>.json'

                    'LastModified': datetime(2015, 1, 1),
                    etc
                        },
                        etc (one such dict for each object)
                            ]
                'Name': 'string',       # the bucket name
                'Prefix': 'string',     # eg 'design' in key
                                        # 'design/<timestamp-here>.json'
                'KeyCount': 123,
                'MaxKeys': 123,
                etc (other keys)
            }

        2) S3_client: a boto3 S3 client.

        3) bucket_name: name of the 
            ingestion bucket.



    Returns:
        A python list of dictionaries
        that represents a whole table, 
        each dictionary representing 
        a row of the table.

    """

    latest_table_key, table_name = make_table_name_and_key(resp_dict)
    

    latest_table = retrieve_latest_table(S3_client,
                                 bucket_name,
                                 latest_table_key,
                                 table_name   
                                       )


    return latest_table











# OLD CODE:
    # try:
    #     # Get the latest
    #     # table itself:
    #     response = S3_client.get_object(Bucket=bucket_name,
    #                                     Key=latest_table_key)
    #     data = response["Body"].read().decode("utf-8")

    #     # Unjsonify the table and return it:
    #     return json.loads(data)

    # except ClientError:
    #     # log the error
    #     # and stop the code:
    #     logger.exception(errors_lookup["err_5"] + f"{table_name}")
    #     raise
