
# OLD CODE:
# clean_table_list = make_data_json_safe(table_dict[table]) # [{<updated-row data>}, {<updated-row data>}, etc]



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


    # Determine whether a 
    # date dimension table 
    # needs to be created 
    # and if yes, make it:
    # should_make_date_dim = make_date_dim_table_if_needed(lookup['proc_bucket'], 
    #                                               lookup['s3_client'],
    #                                               lookup['timestamp_string'],
    #                                               lookup['start_date'],
    #                                               lookup['num_rows']
    #                                               )


    # if code has made a 
    # date dimension table, 
    # put it in the 
    # processed bucket:
    # if should_make_date_dim[0]:
    #     upload_to_s3(lookup['s3_client'], 
    #                  lookup['proc_bucket'], 
    #                  should_make_date_dim[2], 
    #                  should_make_date_dim[1])