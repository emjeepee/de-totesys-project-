


# a lookup table for 
# error messages that 
# first lambda utlilities 
# will employ when logging 

errors_lookup = {
'err_0': "Error caught in change_after_time_timestamp() while trying read ingestion bucket for previous timestamp.",    
"err_1": "Error caught in change_after_time_timestamp() while trying to write new timestamp to ingestion bucket.",
"err_2": "Error caught in get_column_names() while trying to read from the totesys database the table ",
"err_3": "Error caught in get_updated_rows() while trying to read from the totesys database the table ",
"err_4": "Error caught in get_most_recent_table_data() while trying to read ingestion bucket for the latest table of name ",
"err_5": "Error caught in get_latest_table() while trying to read ingestion bucket for the latest table under key ",
"err_6": "Error caught in save_updated_table_to_S3() while trying to write to the ingestion bucket upodated table ",
"err_7": "Error caught in write_to_s3() while trying to write to the ingestion bucket table ",
"err_8": "Error caught in close_db() while trying to close the connection to the database",
                }


