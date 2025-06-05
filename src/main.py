# imports

# def lambda_handler():
#     conn = conn_to_db()
#     conn.run('some sql to select the table names')
#     # convert the above into a list of table names

#     # pull in the last time this was run (last datetime) (datetime.now) - 5 minutes
#     # check the s3 for a timestamp (, compare it to datetime.now)
#     # set after_time = timestamp on the s3, maybe do some formatting to make it work for the query

#     for table in talble_names:
#         table_data = read_table(table, conn, after_time)
#         json_data = convert_data(table_data)
    
#     # compile json_data into dict or something

#     upload_to_s3(compiled_data) # also include a datetime.now? can then pull it down on line 41