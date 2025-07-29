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
           a datetime.datetime object.    
        3) converts the passed-in object
           to a string if it is a
           decimal.Decimal object.    


    Args:
        obj: an object that is either a 
        datetime.datetime object or 
        a decimal.Decimal object.

    Returns:
        a string.
    """
    if isinstance(obj, (datetime.datetime)):
        return obj.isoformat()  # Convert to format '2025-07-01T15:33:47'
    if isinstance(obj, decimal.Decimal):
        return str(obj)
