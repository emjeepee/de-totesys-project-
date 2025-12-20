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
            into dictionaries that
            represent table rows 
            and puts the 
            dictionaries into a 
            list, the list can be 
            converted to json 
            without problem. 
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
          vi)  Booleans True or False                  
                to strings 'TRUE' or 
                'FALSE'     
        4) returns unchanged a 
         passed-in:
         i)  non-json string
         ii) int

    Args:
        val: will be one of 
         these types:
         i)   a datetime.datetime object
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
    
    # NOTE: class bool is a subclass of 
    # int, so you need the code below!! 
    # Don't change ints but 
    # True -> 'TRUE' and 
    # False -> 'FALSE':    
    if isinstance(val, int):
        if isinstance(val, bool):
            if val:
                return 'TRUE'
            else:
                return 'FALSE'
        else:
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

    # Boolean -> string version, all caps;        


