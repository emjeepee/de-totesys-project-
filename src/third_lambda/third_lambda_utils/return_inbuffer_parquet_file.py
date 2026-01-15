from io import BytesIO



def return_inbuffer_parquet_file(s3_client,
                                 object_key: str,
                                 bucket: str
                                 ):

    """
    This function:
        looks in the processed
        bucket and from it gets
        a buffer that contains
        a table that is in the 
        form of Parquet file.
    
    
    Args:
        s3_client: a boto3 S3 
        client

        object_key: the key 
        under which the 
        processed bucket stores 
        the table of interest

        bucket: the name of the 
        processed bucket.
        
    Returns:
        a buffer containing 
        a table in Parquet form.

    """


    dict_from_s3 = s3_client.get_object(Key=object_key, 
                                        Bucket=bucket)
    raw_bytes = dict_from_s3["Body"].read()
    pq_buff = BytesIO(raw_bytes)
    return pq_buff