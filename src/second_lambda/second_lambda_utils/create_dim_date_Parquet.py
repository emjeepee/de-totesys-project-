from second_lambda.second_lambda_utils.make_dim_date_python import make_dim_date_python
from src.second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquet








def create_dim_date_Parquet(start_date, timestamp_string: str):
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

    Returns: 
        A list. The member of the list at 
        index 0 is the date dimension table 
        as a Parquet file. The member at 
        index 1 is the key (a string) under 
        which the processed bucket will 
        store the table.
    """            
    
    

    # Make a date dimension table as a
    # Python list of dictionaries, 
    # convert it to parquet form and 
    # return it along ith the timestamp:
    dim_date_py = make_dim_date_python(start_date, 2557) # a list of dictionaries
    dim_date_pq = convert_to_parquet(dim_date_py)
    dim_date_key = f"{timestamp_string}/dim_date.parquet" # "2025-08-11_15-42-10/dim_date.parquet"
    return [dim_date_pq, dim_date_key]
        
    
