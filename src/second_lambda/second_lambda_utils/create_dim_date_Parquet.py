from .make_dim_date_python import make_dim_date_python
from ....zz_to_dump.convert_to_parquetOLD import convert_to_parquet








def create_dim_date_Parquet(start_date, timestamp_string: str, num_rows: int):
    """
    This function: 
        creates a date dimension table as a 
         Python list of dictionaries and
         converts that list to Parquet form.
            
    Args:
        1) start_date: a datetime object for 
            current time (minus seconds).
        2) timestamp_string: holds the date.
            For example "2025-08-14_12-33-27" 
            for 12.33pm and 27 secs, 14Aug2025.
        3) num_rows: the number of rows that
            the date dimension table will 
            have. Equal to the number of days 
            the table will cover. This is also 
            the number of days into the past
            from today that the date dimension  
            table will cover.

    Returns: 
        A list. The member at index 0 is 
        the date dimension table as a 
        Parquet file. The member at index 
        1 is the key (a string) under 
        which the processed bucket will 
        store the table.
    
    """            
    
    

    # Make a date dimension table as a
    # Python list of dictionaries.
    # The date dimensions table has a 
    # row for each day. The second arg 
    # below is the number of days or 
    # rows in the table from start_date: 
    dim_date_py = make_dim_date_python(start_date, num_rows) # a list of dictionaries
    
    # Convert the list to Parquet form: 
    dim_date_pq = convert_to_parquet(dim_date_py)
    
    # Make a key under which to store 
    # the Parquet file in the S3 
    # processed bucket:
    dim_date_key = f"dim_date/{timestamp_string}.parquet" # "dim_date/2025-08-11_15-42-10.parquet"
    
    # Return Parquet file and timestamp
    # in a list:
    return [dim_date_pq, dim_date_key]       
    
