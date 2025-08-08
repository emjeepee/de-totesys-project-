import boto3
from botocore.exceptions import ClientError

def save_updated_table_to_S3(
    updated_table, S3_client: boto3.client, new_key: str, bucket: str
                            ):
    """
    This function:
        1. takes the updated table and stores
            it in the S3 ingestion bucket
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
        bucket: a string that is the name of the
            S3 ingeastion bucket
    """
    try:
        S3_client.put_object(Bucket=bucket, Key=new_key, Body=updated_table)
    except ClientError as e:
        raise RuntimeError('Error occurred in attempt to write data to the ingestion bucket') from e
