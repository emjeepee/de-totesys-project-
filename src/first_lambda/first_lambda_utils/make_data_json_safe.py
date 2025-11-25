import copy

from datetime import datetime
from decimal import Decimal

from .change_vals_to_strings import change_vals_to_strings




def make_data_json_safe(table_data):
    """
    This function:
         1. converts data in a list 
         of dictionaries to formats 
         that will allow a later 
         function to convert the 
         list to json. The list
         represents a table from
         the totesys database.

    args:
        table_data: a pythom list of
        dictionaries that represents
        a table, each dictionary 
        representing a row. The 
        table has come from the 
        totesys database. The 
        key-value pairs of the 
        dictionaries are 
        columname-filedvalue 
        pairs. 

    returns:
        the same list of dictionaries 
        as received as argument but 
        with the values of the 
        key-value pairs of the 
        dictionaries converted to 
        formats that will permit a 
        later function to convert the
        list into json. 

    """
    # values that this function must convert:
    # datetime -> isoformat string
    # Decimal -> string

    
    lst_deep_copy = copy.deepcopy(table_data) # [{...}, {...}, {...}, etc]

    for dct in lst_deep_copy:
        for ky, val in dct.items(): 
            change_vals_to_strings(ky, val, dct)

    return lst_deep_copy


    