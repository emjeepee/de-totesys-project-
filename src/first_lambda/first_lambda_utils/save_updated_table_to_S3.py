import boto3
import logging

from botocore.exceptions import BotoCoreError
from .handle_errors import handle_errors



logger = logging.getLogger(__name__)




def save_updated_table_to_S3(
    updated_table, S3_client: boto3.client, new_key: str, bucket: str
                            ):
    """
    This function:
        stores an updated table in the S3
        ingestion bucket under the given key.
        
    Args:
        updated_table: a jsonified python list of
            dictionaries. The dictionaries 
            represent the rows of the table. The 
            number of dictionaries equals the 
            number of rows in a whole table.
        S3_client: the boto3 client for S3.
        new_key: a string that is the key under
            which the updated table will be
            saved in the S3 ingestion bucket.
            It takes this form:
            'design/<timestamp-string-here>.json'.
        bucket: a string that is the name of the
            S3 ingestion bucket.
    """

    err_msg = f"Error in function " \
              "\n save_updated_table_to_S3()." \
              "\n Unable to write an updated"  \
              "\n table to the ingestion bucket." \


    try:
        S3_client.put_object(Bucket=bucket, Key=new_key, Body=updated_table)
        logger.info("Function save_updated_table_to_S3() successfully\n wrote an updated table to the ingestion bucket")
    except BotoCoreError as e:
        handle_errors(e, logger, err_msg)
