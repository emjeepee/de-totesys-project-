from src.first_lambda.first_lambda_utils.serialise_datetime import convert_dt_values_to_iso
from src.first_lambda.first_lambda_utils.make_row_dicts import make_row_dicts

from pg8000.native import Connection
import datetime
import logging


logger = logging.getLogger("MyLogger")


def read_table(table_name: str, conn: Connection, after_time: str):
    """
    This function:
         gets all data from all rows in a table
         in the ToteSys database if those rows 
         contain data that were updated after a
         time that is passed in (ie after_time).

    Args:
         table_name: the name of the table.
         conn: an instance of a pg8000:native 
          Connection object.
         after_time: a time stamp that will always 
          be the time of the last run of the 
          first lambda function.

    Returns:
         a dict such as {"sales": [
                                        {"Name": "xx", "Month": "January", "Value": 123.45, etc}, <-- data from one row
                                        {"Name": "yy", "Month": "January", "Value": 223.45, etc}, <-- data from one row
                                        {"Name": "zz", "Month": "January", "Value": 323.45, etc}, <-- data from one row
                                        etc
                                  ] 
                        },
        where "sales" is the table name and "Name", "Month", "Value", 
        etc are the table's column names and the values of those keys
        are the row-cell values.
    """

    # Get only those rows from the table
    # that contain data updated after 
    # previous run of the first
    # lambda function. Each table has a 
    # column labelled 'last_updated'.
    # result will contain a list of 
    # member lists, each member
    # representing a row, eg:
    # [ [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), datetime.datetime(2025, 6, 4, 8, 58, 10, 6000)],
    # [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), datetime.datetime(2025, 6, 4, 9, 26, 9, 972000)],
    # [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), datetime.datetime(2025, 6, 4, 9, 29, 10, 166000)], etc  ]
    result = conn.run(
        f"""
        SELECT * FROM {table_name}
        WHERE last_updated > :after_time LIMIT 20;
        """,
        after_time=after_time,
                    )



    # Make a list of the column names of the
    # table in question. 
    # query_result below will be a list 
    # of lists, like this (because that is what 
    # pg8000.native.Connection returns – other 
    # versions of pg8000 return tuples and can be 
    # made to return dictionaries):
    # [
    #     ['design_id'],
    #     ['created_at'],
    #     ['design_name'],
    #     ['file_location'],
    #     ['file_name'],
    #     ['last_updated'],
    # ] 
    query_result = conn.run(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
                           )

    # Convert query_result to a list 
    # of column-name strings: 
    column_names = [col[0] for col in query_result] # ['name', 'location', etc]


    # convert those values in the member lists of result
    # that are datetime.datetime objects into ISO time 
    # strings and convert those values that are 
    # decimal.Decimal values into floats:
    result_washed = convert_dt_values_to_iso(result) 

    row_data = make_row_dicts(column_names, result_washed)
    # row_data ends up looking like 
    # [ {"id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00', etc},         
    #   {"id": 7,  "name": "bbb",  "value": 3.15,  "date": '2024-05-01T10:30:00', etc},
    #    etc ]
    # ie it's a list that contains dicts. each dict
    # represents a row of updated data and each 
    # key-value pair in a dict represents <column name>:<cell value>.


    return {table_name: row_data}

