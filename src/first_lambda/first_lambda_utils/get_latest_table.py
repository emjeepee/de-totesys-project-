import json
import boto3
from botocore.exceptions import ClientError


def get_latest_table(resp_dict, S3_client: boto3.client, bucket_name: str):
    """
    This function:
        1) has the overall responsibility of 
            getting from the ingestion bucket 
            data that represents the most 
            recently saved table.
        2) gets the key of the most recent 
            version of the table in the 
            ingestion bucket (the table being 
            in the form of a jsonified Python
            list of dictionaries).
        3) once it knows the key gets the 
            data in the bucket saved under 
            that key and returns it as a 
            Python list.
        4) gets called by 
            get_most_recent_table_data().

    Args:
        1) resp_obj: a dictionary that boto3 
            method S3_client.list_objects_v2()
            returns. Looks like this:
            {
                'IsTruncated': True|False,
                'Contents': [
                        {
                    'Key': 'string',  # objects of interest are stored
                                      # under this key, which looks 
                                      # like this: 
                                      # 'design/<timestamp-here>.json'
                                      
                    'LastModified': datetime(2015, 1, 1),
                    etc 
                        },
                        etc (one such dict for each object)
                            ]
                'Name': 'string',       # the bucket name
                'Prefix': 'string',     # eg 'design' in key 'design/<timestamp-here>.json'
                'KeyCount': 123,
                'MaxKeys': 123,
                etc (other keys)
            }    

        2) S3_client: a boto3 S3 client.
        3) bucket_name: name of the ingestion bucket.
        
    Returns:
        A python list of dictionaries. The list 
         represents a whole table. Each dictionary 
         represents a row of the table.        

    """
    try:
        # Get the list of keys under which
        # the versions of the table are 
        # stored:
        keys_list = [dict["Key"] for dict in resp_dict.get("Contents", [])]
        # ['design/2025-06-02_22-17-19-2513.json', 'design/2025-05-29_22-17-19-2513.json', etc]
        
        # Get the key for the latest table:
        latest_table_key = sorted(keys_list)[ -1 ]  # 'design/2025-06-02_22-17-19-2513.json'
        
        # Get the latest table itself:
        response = S3_client.get_object(Bucket=bucket_name, Key=latest_table_key)
        data = response["Body"].read().decode("utf-8")
        
        # Unjsonify the table and return it:
        return json.loads(data)

    except ClientError as e:
        raise RuntimeError('Error occurred in reading the ingestion bucket') from e

        