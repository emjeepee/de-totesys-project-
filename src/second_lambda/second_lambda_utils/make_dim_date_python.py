from datetime import timedelta
import calendar






def make_dim_date_python(start_date, loop_range: int, ):
    """
    This function:
        Creates a list of dictionaries 
         that represents the date 
         dimension table. 
         All dictionaries have the key 
         date_id. In the dictionary at 
         index 0 the value ofthe key 
         is value is datetime object 
         start_date. Successive 
         dictionaries have date_id 
         values that increment by one 
         day, ie start_date + 1 day, 
         start_date + 2 days, 
         start_date + 3 days, etc.

    Args:
        start_date: a datetime object 
         for the date from which the 
         date dimension table must begin. 

        loop_range: a number of days 
         from loop_date. Also the number 
         of dictionaries that will
         populate the list that 
         represents the date dimension 
         table (ie the number of rows).

    Returns:
        A date dimension table as a 
         list of dictionaries that looks 
         like this:
        [ {"date_id": start_date, "year": 2020, "month": 07, "day": 2 ... etc},
          {"date_id": -start_date+1day-, "year": 2020, "month": 07, "day": 3 ... etc},
          {"date_id": -start_date+2days-, "year": 2020, "month": 07, "day": 4 ... etc},
          etc 
        ], 
        If loop_range is, eg, 5000, there 
        will be 5000 member dictionaries 
        in the list.

    """

    dim_date = []
    # Leap years = 24, 28.
    # Calculate days from 1Jan2024-31Dec2030:
    # 366 + 366 (for years 24 and 28)
    # plus 5 * 365 (for the other years)
    # equals 2557 days. 
    
    # Doing this in row below: 
    # "day_name": start_date.strftime("%B"),     
    # "month_name": start_date.strftime("%B"), 
    # makes values German for German locale.
    # module calendar guarantees English.
    for i in range(loop_range):
        row = {                                                          # 19Aug25 --HAVE CHECKED THESE:
                "date_id": start_date.date(),                            # is datetime.date object. In warehouse, must be SQL date
                "year": start_date.year,                                 # is int. In warehouse, must be SQL INT
                "month": start_date.month,                               # is int (1 for January). In warehouse, must be SQL INT
                "day": start_date.day,                                   # is int. In warehouse, must be SQL INT
                "day_of_week": start_date.weekday() + 1,                 # is int (1 for Monday). In warehouse, must be SQL INT
                "day_name": calendar.day_name[start_date.weekday()],     # is str (eg 'Monday'). In warehouse, must be SQL VARCHAR
                "month_name": calendar.month_name[start_date.month],     # is str (eg 'January'). In warehouse, must be SQL VARCHAR
                "quarter": (start_date.month - 1) // 3 + 1               # is int. In warehouse, must be SQL INT
              }

        dim_date.append(row)
        start_date += timedelta(days=1)

    return dim_date














# IMPORTANTS!!!!!!
# the date_ID value should be a datetime object so that 
# pandas recognises it as a date:
# date_dimensions_list = [
#     {
#         "date_id": date(2025, 8, 11),
#         "year": 2025,
#         "month": 8,
#         "day": 11
#     },
#     {
#         "date_id": date(2025, 8, 12),
#         "year": 2025,
#         "month": 8,
#         "day": 12
#     }
# ]



# OLD CODE:
    # # Make two datetime objects, one for a 
    # # start date of 1Jan2000, the other for
    # # an end date of 31Dec2030. Both contain
    # # date information only:
    # start = datetime.fromisoformat("2000-01-01").date()
    # end = datetime.fromisoformat("2030-12-31").date()

    # dim_date = []

    # while current_date <= end:
    #         row = {
    #             "date_id": date_id,
    #             "year": current_date.year,
    #             "month": current_date.month,
    #             "day": current_date.day,
    #             "day_of_week": current_date.isoweekday(),
    #             "day_name": current_date.strftime("%A"),
    #             "month_name": current_date.strftime("%B"),
    #             "quarter": (current_date.month - 1) // 3 + 1,
    #         }
    #         dim_date.append(row)
    #         current_date += timedelta(days=1)
    #         date_id += 1
