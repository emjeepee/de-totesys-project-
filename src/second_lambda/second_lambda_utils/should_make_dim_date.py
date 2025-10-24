





def should_make_dim_date(ifrop, cddP, uts3, start_date, timestamp_string, num_rows, proc_bucket, s3_client):
    """
    This function:
      1) determines whether this is the first ever
          run of the pipeline.
      2) creates a date dimension table in 
          Parquet form if it is the first ever 
          run.
      3) uploads the Parque file to the processed 
          bucket.
      4) does nothing if this is NOT the first ever
          run of the pipeline.

    Args:
        1) ifrop: utility function 
            is_first_run_of_pipeline().
        2) cddP: utility function 
            create_dim_date_Parquet().
        3) uts3:             
        4) start_date: a datetime object 
            for the date from which the 
            date dimension table should 
            start.
        5) timestamp_string: a string 
            that will form part of the 
            key under which to store 
            the date dimension table in 
            the processed bucket
        6) num_rows: the number of rows 
            that the date dimension table 
            should have (ie the number of 
            days the table should cover 
            from the start date).             
        7) proc_bucket: the name of the 
            processed bucket
        8) s3_client: boto3 s3 client 
            object.            

    Returns:
        None            
    
    """

    try:
        # if it is the first ever run of the pipeline:
        if ifrop(proc_bucket, s3_client):
            # make the date dimension table and 
            # a key for it and put them in an array:
            arr = cddP(start_date, timestamp_string, num_rows)

            # store the date dimension table 
            # in the processed bucket under the
            # key:
            uts3(s3_client, proc_bucket, arr[1], arr[0])
    
    except RuntimeError as e:
        raise RuntimeError from e           