from datetime import datetime, date, time









def test_func_1():
    s_1 = "2025-11-14T15:17:08"
    s_2 = "2025-11-14_15:17:08"
    s_3 = "2025-11-14 15:17:08"
    
    dt_1 = datetime.fromisoformat(s_1)
    dt_2 = datetime.fromisoformat(s_2)
    dt_3 = datetime.fromisoformat(s_3)

    dt_date_1 = dt_1.date()
    dt_date_2 = dt_2.date()
    dt_date_3 = dt_3.date()

    dt_time_1 = dt_1.time()
    dt_time_2 = dt_2.time()
    dt_time_3 = dt_3.time()

    # print(f"dt_date_1 is {dt_date_1}")
    # print(f"dt_date_2 is {dt_date_2}")
    # print(f"dt_date_3 is {dt_date_3}")
    # print(f"dt_time_1 is {dt_time_1}")
    # print(f"dt_time_2 is {dt_time_2}")
    # print(f"dt_time_3 is {dt_time_3}")

    date_iso = dt_date_1.isoformat()
    time_iso = dt_time_1.isoformat()

    print(f"date_iso is {date_iso} and time_iso is {time_iso}")


test_func_1()

