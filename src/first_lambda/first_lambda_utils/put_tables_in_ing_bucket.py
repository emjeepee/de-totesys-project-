from . write_tables_to_ing_buck import write_tables_to_ing_buck
from .make_one_updated_table import make_one_updated_table

def put_tables_in_ing_bucket(is_first_run: bool,
                             s3_client,       
                             bucket_name: str,                            
                             data_for_s3: list
                            ):
    
    """
    This function:
        1) determines whether 
        this is the first ever 
        run of the pipeline.

        2) saves the passed-in 
        list of tables to the 
        ingestion bucket if it 
        is the first ever run 

        3) makes updated tables 
        if it is the 2nd-plus 
        run of the pipeline and 
        saves the updated tables 
        to the ingestion bucket

        
    Args:
        is_first_run: a boolean
        whose value is True if
        this is the first ever
        run of the pipeline and
        False otherwise

        s3_client: boto3 S3 client

        bucket_name: the name of
        the ingestion bucket

        data_for_s3: a list of
        dictionaries each of
        which is a table. This
        list takes this form:
         [
        {'design': [{<updated-row data>}, etc]},
        {'sales': [{<updated-row data>}, etc]},
        etc
         ]

    Returns:
        None
   
    """

    # if first ever run of 
    # the pipeline, write
    # all tables to the
    # ingestion bucket:
    if is_first_run:
        to_save_to_bucket = data_for_s3

    # if 2nd-plus run of 
    # pipeline, update the 
    # tables, then write them 
    # to the ingestion bucket:
    if not is_first_run:
        to_save_to_bucket = [make_one_updated_table(member,
                                                    s3_client,
                                                    bucket_name)
                             for member in data_for_s3 ]
        
    write_tables_to_ing_buck(s3_client, 
                             bucket_name, 
                             to_save_to_bucket
                            )
