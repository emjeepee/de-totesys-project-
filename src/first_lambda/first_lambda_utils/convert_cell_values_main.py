from datetime import datetime
import json
from json.decoder import JSONDecodeError

from decimal import Decimal

from src.first_lambda.first_lambda_utils.convert_cell_values_aux import convert_cell_values_aux



def convert_cell_values_main(val):
    """
    This function:
        converts a passed-in value into 
        another type depending on the 
        type of the passed-in value.
        It converts the passed-in value 
        to a string if the passed-in 
        value is 
         i)   a datetime object.
         ii)  a json string.
         If the passed-in value is 
         a decimal.Decimal object
         this function returns 
         a float.
         If the passed-in value is a 
         non-json string or an int
         this function returns the 
         passed-in value unchanged.         

    Args:
        val: is either
         i)   a datetime object
         ii)  a json string
         iii) a Decimal object
         iv)  a non-json string
         v)   an int  

    Return:
        a string if the passed-in val
         is a datetime object, a json 
         string or a non-json string.
        a float if the passed-in val 
         is a decimal.Decimal object.
        the passed-in val unchanged 
         if the passed-in val is an 
         int.
        the passed-in val unchanged 
         if the passed-in val is a 
         non-json string.
    """



    if isinstance(val, str) or isinstance(val, datetime) or isinstance(val, Decimal) or isinstance(val, int) or val == None:  
        # If val is a json string
        # return it otherwise
        # call 
        # convert_cell_values_aux(val):
        try:
            return_val = json.loads(val)
            return return_val

        except (JSONDecodeError, TypeError): 
            return convert_cell_values_aux(val)

