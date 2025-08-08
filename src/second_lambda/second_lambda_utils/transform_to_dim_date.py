import boto3
import json
from datetime import datetime, timedelta
import boto3.exceptions
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from currency_codes import get_currency_by_code, Currency




def transform_to_dim_date(start_date=None, end_date=None):
    """
    This function:
        Creates a list of dictionaries that looks like this:
        [ {"date_id": 1, "year": 2025, "month": 'July', ... 'quarter': 3},
          {"date_id": 2, "year": 2025, "month": 'August', ... 'quarter': 3},
          ...
          {"date_id": <n>, "year": 2025, "month": 'March', ... 'quarter': 1}  
        ]

    Args:
        None.

    Returns:
        The list this function creates, which represents
        the date dimensions table.        

    """

        # Mukund: The two if-else block below look like data cleaning:
    if start_date is None:
            # set to 2000
            start = datetime.fromisoformat("2000-01-01").date()
            # Above datetime.fromisoformat("2000-01-01") converts
            # the string into a datetime object representing 
            # midnight on January 1st, 2000.
    else:
            start = datetime.fromisoformat(start_date).date()
    if end_date is None:
            # set to 2030
            # end_date = datetime.today().date().isoformat()
            end = datetime.fromisoformat("2030-12-31").date()
    else:
            end = datetime.fromisoformat(end_date).date()

        # start = datetime.fromisoformat(start_date).date()
        # end = datetime.fromisoformat(end_date).date()

    if start > end:
            raise ValueError("start_date cannot be after end_date")

    dim_date = []
    current_date = start
    date_id = 1

    while current_date <= end:
            row = {
                "date_id": date_id,
                "year": current_date.year,
                "month": current_date.month,
                "day": current_date.day,
                "day_of_week": current_date.isoweekday(),
                "day_name": current_date.strftime("%A"),
                "month_name": current_date.strftime("%B"),
                "quarter": (current_date.month - 1) // 3 + 1,
            }
            dim_date.append(row)
            current_date += timedelta(days=1)
            date_id += 1
    return dim_date
