from botocore.exceptions import ClientError

import logging




logger = logging.getLogger(__name__)


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

            

    err_msg = "An error occurred in is_first_run_of_pipeline() in attempt to list objects in the processed bucket"

    try:
        objects_list = s3_client.list_objects_v2(Bucket=proc_bucket)

    except ClientError:
        logger.error(err_msg)
        raise 

    
    
    if objects_list["KeyCount"] == 0:
        # print(f"MY_INFO >>>>> In function is_first_run_of_pipeline. The processed bucket is empty")    
        return True
    
    # print(f"MY_INFO >>>>> In function is_first_run_of_pipeline. The processed bucket is NOT empty")    
    return False        

