from datetime import datetime, timedelta


def dt_splitter(input_dt):
    """
    This function:
        1) converts a datetime object into 
            an ISO format timestamp.
        2) Splits the ISO format timestamp 
            into a date string and a time 
            string.

    Args:
        input_dt: a datetime object.
        
    Returns:
        A dictionary that has keys "date" 
         and "time", whose values are 
         a date string and a time string,
         respectively.
    """
    dt = datetime.fromisoformat(input_dt)
    date = dt.date().isoformat()
    time = dt.time().isoformat()
    return {"date": date, "time": time}
