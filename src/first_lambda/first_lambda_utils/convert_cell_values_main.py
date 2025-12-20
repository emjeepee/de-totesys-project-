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
         i)   a datetime.datetime object.
         ii)  a json string.
        2. Converts a decimal.Decimal
        object into a float.
        3. Leaves a non-json string
        or an int unchanged.

    Args:
        val: is either
         i)   a datetime.datetime object
         ii)  a json string
         iii) a decimal.Decimal object
         iv)  a non-json string
         v)   an int
         vi)  None

    Returns:
        1) an ISO string if the passed-in val
         is a datetime object
        2) a string if val is a json string
        3) a string if val is a non-json string
        4) a float if the passed-in val
         is a decimal.Decimal object.
        5) an int if the passed-in val
         is an int.
    """

    # if val is a string determine
    # i)  if it's a json string. if yes
    #     convert it to Python
    # ii) if other type of string. if yes
    #     pass it to
    #     convert_cell_values_aux():
    if isinstance(val, str):
        try:
            return_val = json.loads(val)
            return return_val

        except (JSONDecodeError, TypeError):
            return convert_cell_values_aux(val)

    # if val is
    # 1) a datetime.datetime object,
    # 2) a decimal.Decimal object,
    # 3) an int or
    # 4) None
    # pass it to convert_cell_values_aux():
    else:
        return convert_cell_values_aux(val)
