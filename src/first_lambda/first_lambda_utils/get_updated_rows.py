from pg8000.native import Error

import logging






logger = logging.getLogger(__name__)


def get_updated_rows(conn_obj, after_time: str, table_name: str):
    """
    This function:
        Makes an SQL query to the ToteSys
         database to get the updated rows
         of a table.
    
    Args:
        1) conn_obj: an instance of the 
         pg8000.native Connection class.        
        2) after_time: a time stamp of the form
          "2025-06-04T08:28:12". If the ToteSys
          database has changed a row of the table
          after this time, this function 
          considers the row updated. 
          On the very first run of this handler 
          after_time has a value that represents 
          1January1900. On subsequent runs its 
          value represents the time of the 
          previous run of the first lambda 
          handler.
        3) table_name: the name of the table in 
         the ToteSys database whose updated rows
         this function wants to fetch.
        
    Returns:
        A list of lists. Each member list 
         represents a row and contains values 
         that are cell values.
    
    """
    

    err_msg = "Error in function get_updated_rows()." \
              "\nFailed to read ToteSys database" \
              "\nwhen trying to get a list of updated" \
              "\nrows for table {table_name}."

    query = f"SELECT * FROM {table_name} WHERE last_updated > :after_time LIMIT 20;"

    try: 
        response = conn_obj.run(query, after_time=after_time)
        # log status:
        print("Function get_updated_rows() successfully\n got updated rows of table: %s", table_name)
        logger.info("Function get_updated_rows() successfully\n got updated rows of table: %s", table_name)

        return response
    
    except Error as e:
        logger.error(err_msg)
        raise RuntimeError
        
        
