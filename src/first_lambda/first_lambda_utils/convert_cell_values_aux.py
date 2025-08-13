from datetime import datetime
from decimal import Decimal




def convert_cell_values_aux(val):
    """
    This function:
        gets called by function 
         convert_cell_values_main().
        converts a passed-in:
          i)   datetime object to 
               a string
          ii)  decimal.Decimal 
               object to a float.
        returns unchanged a 
         passed-in:
         i)   non-json string
         ii)  an int
         iii) None.  
         performs the above because 
         the passed-in val will be 
         a cell value from a table
         that has to be converted 
         to json to store in the 
         ingestion bucket. 



    Args:
        val: this will be one of 
         these five types:
         i)   a datetime object
         ii)  a decimal.Decimal
              object
         iii) a non-json string
         iv)  an int 
         v)   None.

    Returns:
        either:
            a string
            an int
            a float
            None.                      
    """

    if isinstance(val, datetime):
        # convert to a string of a particular format:
        return val.strftime("%Y-%m-%dT%H:%M:%S.%f")
    if isinstance(val, Decimal):
        # convert to a float:
        return float(val)
    if isinstance(val, str) or isinstance(val, int) or val is None:
        # return unchanged:
        return val                    
