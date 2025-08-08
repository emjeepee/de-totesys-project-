from src.second_lambda.second_lambda_utils.transform_to_dim_date import transform_to_dim_date
from src.second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquet
from src.second_lambda.second_lambda_utils.upload_to_s3 import upload_to_s3

import datetime





def create_and_save_dim_date(s3_client, bucket_name: str, timestamp):
    """
    This function: 
        1) determines whether the processed bucket is empty
            (signifying that this is the first ever run of
            the ETL pipeline).
        2) creates a date dimension table if the bucket is             
            empty but nothing if the bucket is not empty.
        3) uploads the date dimension table (if created
            here) to the processed bucket
            
    Args:
        1) s3_client: a boto3 s3 client.
        2) bucket_name: the name of the S3 processed bucket.
        3) timestamp: a datetime object representing the 
         current time.


    
    
    """            
    # processed_bucket_name = "11-processed-bucket"
    timestamp = datetime.datetime.now()

    # Check if the processed bucket is empty, and if so generate date.parquet file
    boto_list_processed_objects = s3_client.list_objects_v2(  Bucket=bucket_name   )

    # If the processed bucket is empty
    # make a date dimension table, convert
    # it to parquet form and save it in 
    # the S3 processed bucket:
    if boto_list_processed_objects["KeyCount"] == 0:
        dim_date_py = transform_to_dim_date()
        dim_date_pq = convert_to_parquet(dim_date_py)
        dim_date_key = f"{timestamp}/date.parquet"
        upload_to_s3(s3_client, bucket_name, dim_date_key, dim_date_pq)
        