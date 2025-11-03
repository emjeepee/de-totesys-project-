import pandas as pd
import logging

from io import BytesIO
from botocore.exceptions import ClientError



logger = logging.getLogger(__name__)



def make_pandas_dataframe(proc_bucket, S3_client, object_key):
    """This function:
        1) is the first function that the third lambda 
            function calls. The third lambda function 
            runs in response to an event sent from the 
            processed bucket that tells it that a table
            has been stored there (as a Parquet file)
        2) gets the Parquet file just stored in the 
            processed bucket.
        3) create a Pandas dataframe from the Paruet 
            file.                             

            
    Args:
        proc_bucket: a string for the name of the processed 
         S3 bucket.
        S3_client: the boto3 S3 client object.
        object_key: the key under which the processed 
         bucket stores the Parquet file.

    Returns:
        A pandas dataframe that contains the data from a table.
    """

    err_msg = 'An error occurred in make_pandas_dataframe() while trying to read from the processed bucket'

    try:
        # get the Parquet file in
        # the processed bucket:
        response = S3_client.get_object(Key=object_key, Bucket=proc_bucket)
    except ClientError:
        logger.error(err_msg) 
        raise 

    pq_bytes_file = response["Body"].read()
    parquet_buffer = BytesIO(pq_bytes_file)
    # make the pandas dataframe 
    # and return it (the first column
    # in the dataframe is the primary 
    # key as that was the case in the 
    # Parquet file and the Python list 
    # before that -- this is important
    # because a later utility of the 
    # third lambda handler (that 
    # creates query strings) requires 
    # that to be the case):
    return pd.read_parquet(parquet_buffer, engine="pyarrow")


    






# OLD CODE:
    # # get latest timestamp string
    # # "1900-01-01 00:00:00":
    # latest_ts_hh_mm_ss = get_latest_timestamp(S3_client, ing_bucket, current_ts_key)

    # # get a list of the keys for
    # # the latest tables in the
    # # processed bucket:
    # list_of_keys = get_keys_of_latest_tables(S3_client, proc_bucket, latest_ts_hh_mm_ss)

    # # get the actual objects in
    # # the processed bucket
    # # associated with the keys
    # # in the list just created,
    # # convert them into pandas
    # # dataFrames and put them in
    # # a list:
    # dict_of_dataFrames = get_pq_files(S3_client, list_of_keys, proc_bucket)

    # # dict_of_dataFrames will look
    # # like this:
    # # {
    # # 'dim_date': dim_table.parquet,
    # # ... ,
    # # 'facts_sales_order': facts_sales_order.parquet
    # # }, where each key is the name of a table
    # # in the warehouse database.

    # return dict_of_dataFrames
