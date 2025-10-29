
from pg8000.native import Error

import logging






logger = logging.getLogger(__name__)




def get_column_names(conn_obj, table_name: str):
    """
    This function:
        Makes an SQL query to the ToteSys
         database to get the column names 
         of a table.
    
    Args:
        1) conn_obj: an instance of the 
         pg8000.native Connection class.        
        2) table_name: the name of the 
         table in the ToteSys database 
         from which to get its column 
         names.
        
    Returns:
        A list of lists. Each member list
         contains one string, which is the 
         name of a column.        
    
    """
    

    err_Msg = f"Error in function get_column_names()." \
              "\n Failed to read ToteSys database when" \
              "\n trying to get a list of column names" \
              "\n of table {table_name}."

    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"


    try: 
        response = conn_obj.run(query)
        # log status:
    
        return response
    
    except (Error):
        logger.info(err_Msg)
        raise RuntimeError
        
    

    
        
    