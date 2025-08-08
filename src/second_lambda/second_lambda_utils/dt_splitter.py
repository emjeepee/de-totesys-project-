from datetime import datetime, timedelta


def dt_splitter(input_dt):
    """
    Splits the date from the time of a datetime
    Takes a datetimestamp as an argument
    Returns a dictionary with the keys "date" and "time"
    """
    dt = datetime.fromisoformat(input_dt)
    date = dt.date().isoformat()
    time = dt.time().isoformat()
    return {"date": date, "time": time}
