from datetime import datetime


def make_fake_dp_table():

    base_day = datetime(2025, 11, 13)
    # td_1_day = timedelta(days=1)

    dept_table = [
        {
            "department_id": None,
            "department_name": "Sales",
            "location": "Manchester",
            "manager": "Richard Roma",
            "created_at": None,
            "last_updated": None,
        },
        {
            "department_id": None,
            "department_name": "Purchasing",
            "location": "Manchester",
            "manager": "Naomi Lapaglia",
            "created_at": None,
            "last_updated": None,
        },
        {
            "department_id": None,
            "department_name": "Production",
            "location": "Leeds",
            "manager": "Chester Ming",
            "created_at": None,
            "last_updated": None,
        },
        {
            "department_id": None,
            "department_name": "Dispatch",
            "location": "Leds",
            "manager": "Mark Hanna",
            "created_at": None,
            "last_updated": None,
        },
        {
            "department_id": None,
            "department_name": "Finance",
            "location": "Manchester",
            "manager": "Jordan Belfort",
            "created_at": None,
            "last_updated": None,
        },
        {
            "department_id": None,
            "department_name": "Facilities",
            "location": "Manchester",
            "manager": "Shelley Levene",
            "created_at": None,
            "last_updated": None,
        },
        {
            "department_id": None,
            "department_name": "Communications",
            "location": "Leeds",
            "manager": "Ann Blake",
            "created_at": None,
            "last_updated": None,
        },
        {
            "department_id": None,
            "department_name": "HR",
            "location": "Leeds",
            "manager": "James Link",
            "created_at": None,
            "last_updated": None,
        },
    ]

    for i in range(1, 9):  # 1, 2, 3... 8
        dept_table[i - 1]["department_id"] = i
        dept_table[i - 1]["created_at"] = base_day
        dept_table[i - 1]["last_updated"] = base_day

    return dept_table
