from src.first_lambda.first_lambda_utils.convert_values import convert_values
from src.first_lambda.first_lambda_utils.make_row_dicts import make_row_dicts
from src.first_lambda.first_lambda_utils.contact_tote_sys_db import contact_tote_sys_db


from pg8000.native import Connection
import logging


logger = logging.getLogger("MyLogger")


def read_table(table_name: str, conn: Connection, after_time: str):
    """
    This function:
         gets updated rows from a table in the 
         ToteSys postgreSQL database if those rows 
         were updated after a passed-in time 
         after_time.

    Args:
         table_name: the name of the table.
         conn: an instance of a pg8000:native 
          Connection object.
         after_time: a time stamp. On the very 
          first run of this handler after_time
          represents date 1/1/1900. On subsequent 
          runs it represents the time of the 
          previous run of this handler.

    Returns:
         a dict such as {"sales_order": [
                                        {"Name": "xx", "Month": "January", "Value": 123.45, etc}, <-- data from one row
                                        {"Name": "yy", "Month": "January", "Value": 223.45, etc}, <-- data from one row
                                        {"Name": "zz", "Month": "January", "Value": 323.45, etc}, <-- data from one row
                                        etc
                                  ] 
                        },
        where "sales" is the table name and keys "Name", "Month", "Value", 
        etc are the table's column names and the values of those keys
        are the row-cell values.
    """

    # Get only those rows from the table
    # that contain data updated after 
    # previous run of the first
    # lambda function. Each table has a 
    # column labelled 'last_updated'.
    # query_result_1 will contain a list of 
    # member lists, each member
    # representing a row, eg:
    # [ [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), datetime.datetime(2025, 6, 4, 8, 58, 10, 6000)],
    # [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), datetime.datetime(2025, 6, 4, 9, 26, 9, 972000)],
    # [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), datetime.datetime(2025, 6, 4, 9, 29, 10, 166000)], etc  ]
    
    

    # Make a list of the column names of the
    # table in question. 
    # query_result_2 below will be a list 
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

    try:
        query_result_2 = contact_tote_sys_db(conn, 2, 'not-relevant', table_name) # a list of lists,
                                                                                  # each member list 
                                                                                  # being a column 
                                                                                  # name  
        query_result_1 = contact_tote_sys_db(conn, 1, after_time, table_name) # a list of lists, 
                                                                              # each member list 
                                                                              # representing a row
                                                                              # and containing 
                                                                              # row values only
                                                                              # (without column 
                                                                              # names)
    except RuntimeError as e:
        raise RuntimeError from e
    

    # Convert query_result_2 to a 
    # list of column-name strings: 
    column_names = [col[0] for col in query_result_2] # ['name', 'location', etc]


    # convert those values in the member lists of 
    # query_result_1. Convert:
    # datetime.datetime object -> ISO string
    # Decimal value -> float
    # json -> string:
    result_washed = convert_values(query_result_1) 

    row_data = make_row_dicts(column_names, result_washed)
    # row_data ends up looking like 
    # [ {"id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00', etc},         
    #   {"id": 7,  "name": "bbb",  "value": 3.15,  "date": '2024-05-01T10:30:00', etc},
    #    etc ]
    # ie it's a python list of dicts. each dict
    # represents a row of updated data and each 
    # key-value pair in a dict represents 
    # <column name>:<cell value>.


    return {table_name: row_data}

