from botocore.exceptions import ClientError




def is_first_run_of_pipeline(proc_bucket: str, s3_client):
    """
    This function:
        1) Determines whether there are 
            any objects in the processed 
            bucket.

    Args:
        1) proc_bucket: the name of the 
            processed bucket.
        2) s3_client: a boto3 S3 client 
            object.        

    Returns:
        True if the processed bucket is 
        empty.
        False otherwise.
    """
    try:
        objects_list = s3_client.list_objects_v2(Bucket=proc_bucket)
        if objects_list["KeyCount"] == 0:
            return True
        return False
    except ClientError as e:
        raise RuntimeError("Second lambda encountered an error in attempt to list objects in the processed bucket") from e


