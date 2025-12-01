

err_0 = (
    """\n\n\nEither change_after_time_timestamp() 
    caught an error while trying to read
    the ingestion bucket for the previous 
    timestamp or this is the first ever
    run of the pipeline (which is correct 
    behaviour)."""
                )

err_1 = (
    """\n\n\nchange_after_time_timestamp() 
    caught an error while trying to write 
    new timestamp to ingestion bucket.""")


err_2 = (
    """\n\n\nget_column_names() caught an error 
    while trying to read from the totesys 
    database the table """
        )

err_3 = (
    """\n\n\nget_updated_rows() caught an error 
    while trying to read from the totesys 
    database the table """
        )

err_4 = (
    """\n\n\nget_most_recent_table_data() caught 
    an error while trying to read ingestion 
    bucket for the latest table of name """)

err_5 = (
    """\n\n\nget_latest_table() caught an error 
    while trying to read ingestion bucket 
    for the latest table under key """
    )

err_6 = (
    """\n\n\nsave_updated_table_to_S3() caught an 
    error while trying to write to the 
    ingestion bucket upodated table """)

err_7a = (
    """\n\n\n
    write_to_s3() caught an error while 
    trying to get keys for all objects in the 
    ingestion bucket that have a prefix of 
    """
        )

err_7b = (
    """\n\n\nwrite_to_s3() caught an error while 
    trying to write to the ingestion bucket the
    table of name """
        )

err_8 = (
    """\n\n\nconn_to_db() caught an error while 
    trying to connect to the totesys database."""
        )

err_9 = (
    """\n\n\nclose_db() caught an error while trying 
    to close the connection to the totesys 
    database."""
        )


# a lookup table for 
# error messages that 
# first lambda utlilities 
# will employ when logging 
errors_lookup = {
'err_0': err_0,    
"err_1": err_1,
"err_2": err_2,
"err_3": err_3,
"err_4": err_4,
"err_5": err_5,
"err_6": err_6,
"err_7a": err_7a,
"err_7b": err_7b,
"err_8": err_8,
"err_9": err_9,
                }


