from datetime import datetime, date
import calendar



def xxx():

    obj_1 = datetime.now() # produces an instance of datetime.datetime
    obj_2 = datetime(2024, 1, 1) # produces an instance of datetime.datetime
    obj_3 = date(24, 12, 12) # produces an instance of datetime.date 

    obj_5 = obj_2.year
    obj_6 = obj_2.month
    obj_7 = obj_2.day
    obj_8 = obj_2.weekday

    # calendar.day_name[obj_2.weekday()]

    print(f'datetime.now() is of type {type(obj_1)}')
    print(f'datetime(24, 12, 12) is of type {type(obj_2)}')
    print(f'date(24, 12, 12) is of type {type(obj_3)}')

    print(f'datetime(2024, 1, 1).year is of type {type(obj_5)}')
    print(f'datetime(2024, 1, 1).month is of type {type(obj_6)}')
    print(f'datetime(2024, 1, 1).day is of type {type(obj_7)}')
    print(f'datetime(2024, 1, 1).weekday() is of type {type(obj_8)}')
    # print(f'datetime(2024, 1, 1).weekday() is of type {type(obj_8)}')

    return



xxx()