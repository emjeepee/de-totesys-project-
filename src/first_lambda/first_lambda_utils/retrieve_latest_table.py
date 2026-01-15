import json
import logging

from botocore.exceptions import ClientError
from .errors_lookup      import errors_lookup



# __name__ has value "retrieve_latest_table.py"
logger = logging.getLogger(__name__)





def retrieve_latest_table(S3_client,
                          bucket_name: str,
                          latest_table_key: str,
                          table_name  
                          ):

    
    """
    This function:
        1) tries to get the latest 
        version of a table from 
        the ingestion bucket

        2) logs an exception if 
        the attempt to get the 
        table fails.

    Args:
        S3_client: a boto3 S3 client

        bucket_name: the name of the 
            S3 bucket (the ingestion 
            bucket)
            
        latest_table_key: the key 
            under which the ingestion 
            bucket has stored the 
            table 

        table_name: the name of the 
            table

    Returns:
        Either the table in the 
        form of a list of dictionaries
        or, if the attempt to get the 
        table fails, None.

    """


    try:
        # Get the latest table:
        response = S3_client.get_object(Bucket=bucket_name,
                                        Key=latest_table_key)
        
        data = response["Body"].read().decode("utf-8")

        table_dict = json.loads(data)
        
        return table_dict
    

    except ClientError:
        # log the error
        # and stop the code:
        logger.exception(errors_lookup["err_5"] + f"{table_name}")
        raise
    