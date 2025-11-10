import json

from json.decoder import JSONDecodeError
from .convert_cell_values_aux import convert_cell_values_aux



def convert_cell_values_main(val):
    """
    This function:
        Converts the passed-in value
        into another type:
        1. Converts these vlaues into  
        a string: 
         i)   a datetime object.
         ii)  a json string.
        2. Converts a decimal.Decimal 
        object into a float.
        3. Leaves a non-json string 
        or an int unchanged.         

    Args:
        val: is either
         i)   a datetime object
         ii)  a json string
         iii) a Decimal object
         iv)  a non-json string
         v)   an int
         vi)  None

    Return:
        a string if the passed-in val
         is a datetime object, a json 
         string or a non-json string.
        a float if the passed-in val 
         is a decimal.Decimal object.
        an int if the passed-in val 
         is an int.
    """

    # if val is a string
    # i)  if it's a json string then 
    #     unjsonify it
    # ii) if other type of string pass 
    #     it to 
    #     convert_cell_values_aux():
    if isinstance(val, str): 
        try:
            return_val = json.loads(val)
            return return_val

        except (JSONDecodeError, TypeError): 
            return convert_cell_values_aux(val)

    # if val is a datetime object, 
    # a Decimal object, an int or 
    # None, pass it to
    # convert_cell_values_aux():  
    else: 
        convert_cell_values_aux(val)
