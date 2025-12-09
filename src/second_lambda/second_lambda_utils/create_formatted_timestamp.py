from datetime import datetime, timezone

def create_formatted_timestamp():
    """
    This function:
        1) creates a timestamp 
        string formatted like 
        this:
        '2025-06-13_13-13-13'

    Returns:
        The formatted 
        timestamp string
    """

    # Make a datetime object that includes 
    # the current time and time zone info:
    now_dt_object = datetime.now(timezone.utc)

    formatted_ts = now_dt_object.strftime("%Y-%m-%d_%H-%M-%S")
    
    return formatted_ts