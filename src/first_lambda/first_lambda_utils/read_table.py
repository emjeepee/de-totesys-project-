from src.first_lambda.first_lambda_utils.convert_values import convert_values
from src.first_lambda.first_lambda_utils.make_row_dicts import make_row_dicts
from src.first_lambda.first_lambda_utils.contact_tote_sys_db import contact_tote_sys_db


from pg8000.native import Connection
import logging


logger = logging.getLogger("MyLogger")


def read_table(table_name: str, conn: Connection, after_time: str):
    """
    This function:
         1) makes a dictionary that contains 
            those rows of a table in the ToteSys 
            database that have been updated. The 
            dictionary's sole key is the name 
            of the table. The value of the key is
            a list of dictionaries, each of which
            represents an updated row and 
            contains key-value pairs, each pair 
            showing '<columnName>': '<cellValue>'.    
         2) makes the dictionary above by getting
            i) updated rows from the table in the 
            form of a list of lists, each member 
            list containing cell values only for 
            an updated row (ie no column names)
            ii) the column names for the table in the
            form of a list of lists. Each member list
            contains a string for the name of a column.
         3) uses the two lists of lists in i) and ii)
            above to create the dictionary mentioned 
            in 1) above.

    Args:
         1) table_name: the name of the table.
         2) conn: an instance of a pg8000:native 
          Connection object.
         3) after_time: a time stamp of the form
          "2025-06-04T08:28:12". If the ToteSys
          database has changed a row of the table
          after this time, this function 
          considers the row updated. 
          On the very first run of this handler 
          after_time has a value that represents 
          1January1900. On subsequent runs its 
          value represents the time of the 
          previous run of the first lambda 
          handler.
          

    Returns:
         the dictionary this function creates, which
         looks like this: {
                    "sales_order": [
                        {"Name": "xx", "Month": "January", "Value": 123.45, etc}, <-- one updated row
                        {"Name": "yy", "Month": "January", "Value": 223.45, etc}, <-- one updated row
                        {"Name": "zz", "Month": "January", "Value": 323.45, etc}, <-- one updated row
                                        etc
                                         ] 
                        },
        where "sales_order" is the table 
        name and keys "Name", "Month", 
        "Value", etc are the table's 
        column names. The values of 
        those keys are the row-cell 
        values.

    """

    try:
        # Make a list of the column names of the
        # table in question. 
        query_result_2 = contact_tote_sys_db(conn, 2, 'not-relevant', table_name) # a list of lists,
                                                                                  # each member list 
                                                                                  # containing a string
                                                                                  # that is a column 
                                                                                  # name  
        # Get only those rows from the table
        # that contain updated data: 
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


    # convert cell values in the 
    # updated rows from 
    # datetime.datetime object -> ISO string
    # Decimal value            -> float
    # json                     -> string:
    cleaned_list = convert_values(query_result_1) 

    # Make a dictionaries for each
    # updated row where the 
    # key-value pairs of each
    # dictionary represent 
    # <column-name>: <cell-value>:
    row_data = make_row_dicts(column_names, cleaned_list)
    # row_data looks like this:
    # [ ... 
    #   {"design_id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00.123456', etc},         
    #   {"design_id": 7,  "name": "bbb",  "value": 3.15,  "date": '2024-06-01T10:30:00.123456', etc},
    #   etc ]


    return {table_name: row_data}




# Each table has a 
    # column labelled 'last_updated'.
    # query_result_1 will contain a list 
    # of lists, each member list 
    # representing a row, eg:
    # [ [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), datetime.datetime(2025, 6, 4, 8, 58, 10, 6000)],
    # [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), datetime.datetime(2025, 6, 4, 9, 26, 9, 972000)],
    # [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), datetime.datetime(2025, 6, 4, 9, 29, 10, 166000)], etc  ]

        # query_result_2  will be a list 
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
