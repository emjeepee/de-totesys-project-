

from .is_first_run_of_pipeline import is_first_run_of_pipeline
from .create_dim_date_Parquet import create_dim_date_Parquet


def make_date_dim_table_if_needed(proc_buck, 
                                  s3_client,
                                  timestamp,
                                  start_date,
                                  num_rows
                                  ):
    """
    This function:
      1) determines whether this 
          is the first ever
          run of the pipeline.
      2) creates a date dimension 
          table in Parquet form 
          if it is the first ever 
          run.
      3) does nothing if this is 
          the 2nd-plus run of the 
          pipeline.

    Args:
        1) ifrop: utility function 
            is_first_run_of_pipeline().
        
        2) cddP: utility function 
            create_dim_date_Parquet().
        
        3) start_date: a datetime object 
            for the date from which the 
            date dimension table should 
            start.
        
        4) timestamp_string: a string 
            that will form part of the 
            key under which to store 
            the date dimension table in 
            the processed bucket
        
        5) num_rows: the number of rows 
            that the date dimension table 
            should have (ie the number of 
            days the table should cover 
            from the start date).             
        
        6) proc_bucket: the name of the 
            processed bucket
        
        7) s3_client: boto3 s3 client 
            object.            

    Returns:
        Either:
        1. Boolean value False if 
        there is no need to make
        a date dimension table.
        
        2. A list of three 
        members, where the member 
        at index 0 is boolean 
        value True, the member at 
        index 1 is the Parquet 
        file and the member at 
        index 2 is the key under 
        which to save the dim 
        date table in the 
        processed bucket.
    
    """
    # determine whether this 
    # is the first ever run 
    # of the pipeline:  
    is_first_run = is_first_run_of_pipeline(proc_buck,  
                                            s3_client)
    
    if not is_first_run:
        return [False]
    
    # make key under which 
    # to store the table in the
    # processed buket:
    dim_date_key = f"dim_date/{timestamp}.parquet" # "dim_date/2025-08-11_15-42-10.parquet"
    # make Parquet table
    # and return it:
    pq_table = create_dim_date_Parquet(start_date, 
                                   timestamp, 
                                   num_rows)

    return [True, pq_table, dim_date_key]