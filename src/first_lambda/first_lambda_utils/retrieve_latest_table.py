import json

from botocore.exceptions import ClientError





def retrieve_latest_table(S3_client,
                          bucket_name: str,
                          latest_table_key: str,
                          logger,
                          errors_lookup: dict,
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

        logger: a logging object

        errors_lookup: a lookup table 
            of error messages 

        table_name: the name of the 
            table

    Returns:
        Either the table in the 
        form of a ;ist of dictionaries
        of, if the attempt to get the 
        table fails, None.

    """


    try:
        # Get the latest table:
        response = S3_client.get_object(Bucket=bucket_name,
                                        Key=latest_table_key)
        
        data = response["Body"].read().decode("utf-8")

        # Convert the table 
        # to python and return 
        # it:
        return json.loads(data)

    except ClientError:
        # log the error
        # and stop the code:
        logger.exception(errors_lookup["err_5"] + f"{table_name}")
        raise
    