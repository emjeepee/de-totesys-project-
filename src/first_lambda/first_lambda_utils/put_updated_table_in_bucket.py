import json

from .save_updated_table_to_S3 import save_updated_table_to_S3


def put_updated_table_in_bucket(table_dict: dict,
                                timestamp: str,
                                s3_client,
                                bucket_name: str):
        
    """
    This function:
        1) Makes the parameters 
            that function
            save_updated_table_to_S3()
            requires

        2) calls function
            save_updated_table_to_S3(),
            and passes in to it the 
            parameters created 
    
    Args:
        table_dict: a dictionary 
            that represents a table. 
            The sole key of this 
            dictionary is the name of 
            the table and the value of  
            the key is a list of 
            dictionaries, each 
            dictionary representing a 
            row and whose key-value 
            pairs are 
            columnname-fieldvalue
            pairs.
        
        timestamp: a timestamp 
        
        s3_client: a boto3 s3 client
        
        bucket_name: name of the S3 
            bucket (the ingestion 
            bucket)

    Returns:
        None        
          
    """


    table_name = list(table_dict.keys())[0]  # 'design'

    # make a list of the 
    # rows of the table:
    list_of_rows = table_dict[table_name]

    # convert the list of
    # rows to json:
    json_list_of_rows = json.dumps(list_of_rows)

    # make key under which
    # to store the table in
    # the ingestion bucket:
    table_key = f"{table_name}/{timestamp}.json" 
                                            # 'sales_order/<timestamp>.json'

    save_updated_table_to_S3(
                            json_list_of_rows,
                            s3_client,
                            table_key,
                            bucket_name  # name of ingestion bucket
                              )
