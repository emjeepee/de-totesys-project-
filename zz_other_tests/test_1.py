

    # should_make_dim_date(is_first_run_of_pipeline, 
    #                     create_dim_date_Parquet, 
    #                     upload_to_s3, 
    #                     lookup['start_date'], # a datetime object for 1 Jan 2024 
    #                     lookup['timestamp_string'], # a timestamp string 
    #                     lookup['num_rows'], # number of rows in date dimension table 
    #                     lookup['proc_bucket'], # name of processed bucket 
    #                     lookup['s3_client']) # boto3 S3 client





# +===============================
# OLD CODE OLD CODE OLD CODE:
    # write updated row data 
    # from each table to the 
    # ingestion bucket: 
    # write_to_s3(data_for_s3, 
    #             lookup['s3_client'], # boto3 S3 client object, 
    #             write_to_ingestion_bucket, 
    #             lookup['ing_bucket_name'])
  # OLD CODE OLD CODE OLD CODE
# +===============================



        # dict_from_s3 = lookup['s3_client'].get_object(Key=lookup['object_key'], Bucket=lookup['proc_bucket'])
        # strmng_bod_obj = dict_from_s3["Body"]
        # raw_bytes = strmng_bod_obj.read()
        # pq_buff = io.BytesIO(raw_bytes) 
