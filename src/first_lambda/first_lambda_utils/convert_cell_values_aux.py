from datetime import datetime
from decimal import Decimal

import re



def convert_cell_values_aux(val):
    """
    This function:
        1) acts to ensure table 
            values will be of the 
            correct form so that 
            when code puts them 
            into dictionaries 
            that become members of 
            a list, the list can be 
            converted to json without 
            problem. 
        2) gets called by function 
         convert_cell_values_main().
        3) converts a passed-in:
          i)   datetime.datetme object 
               to a string.
          ii)  decimal.Decimal 
               object to a float.
          iii) None to 'no data'.
          iv)  string comprising
                spaces only to 
                'no data'.                
          v)   any empty string (ie '') 
                to 'no data'.
        4) returns unchanged a 
         passed-in:
         i)   a non-json string
         ii)  an int

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
            a float.                      
    """

    # convert datetime objects to
    # a string of a certain format:
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%dT%H:%M:%S.%f")
    
    # Decimal -> floats:
    if isinstance(val, Decimal):
        return float(val)
    
    # Don't change ints:    
    if isinstance(val, int):
        return val

    # run of spaces or '' -> 'no data'
    if isinstance(val, str):
        if bool(re.fullmatch(r" *", val)):
            return 'no data'
        else: # don't change other types of string:
            return val
        
    # None -> 'no data';        
    if val is None:
        return 'no data'


# isinstance(val, datetime) or isinstance(val, Decimal) or isinstance(val, int) or val == None:  