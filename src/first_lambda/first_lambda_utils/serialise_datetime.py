import datetime
import decimal

import decimal


def serialise_datetime(obj):
    """
    This function:
        1) gets called by json.dumps() inside 
            convert_data().
        2) converts the passed-in object
            to an ISO date string if it is
            a datetime.datetime object
            and returns the IS form.    
        3) converts the passed-in object
            to a float if it is a
            decimal.Decimal object and 
            returns the float.    
        4) returns the object if it is neither 
            a datetime.datetime object nor a
            decimal.Decimal object.



    Args:
        obj: an object that can be a string, 
        an int, a datetime.datetime object or 
        a decimal.Decimal object or.

    Returns:
        either a string, a float or the
        object passed in.
    """
    if isinstance(obj, (datetime.datetime)):
        return obj.isoformat()  # Convert to format '2025-07-01T15:33:47'
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    else:
        return obj