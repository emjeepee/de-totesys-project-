from src.first_lambda.first_lambda_utils.convert_values import convert_values
from src.first_lambda.first_lambda_utils.make_row_dicts import make_row_dicts
from src.first_lambda.first_lambda_utils.get_updated_rows import get_updated_rows
from src.first_lambda.first_lambda_utils.get_column_names import get_column_names


from pg8000.native import Connection
from src.first_lambda.first_lambda_utils.get_env_vars import get_env_vars

import os


from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db




import logging

logger = logging.getLogger(__name__)


def read_table(table_name: str, conn: Connection, after_time: str):
    """
    This function:
         1) makes a dictionary that 
            contains those rows of a 
            table in the ToteSys 
            database that have been 
            updated. The dictionary's 
            sole key is the name of 
            the table. The value of 
            the key is a list of 
            dictionaries, each of 
            which represents an 
            updated row and contains 
            key-value pairs, each of 
            which is a 
            columnname-fieldvalue
            pair.    
         2) makes the dictionary 
            above by getting
            i) updated rows from the 
            table in the form of a 
            list of lists, each member 
            list containing only cell 
            values for an updated row 
            (ie no column names)
            ii) the column names for 
            the table in the form of 
            a list of lists. Each 
            member list contains a 
            string for the name of a 
            column.
         3) uses the two lists of 
            lists in i) and ii) above 
            to create the dictionary 
            mentioned in 1) above.

    Args:
         1) table_name: the name 
          of the table.

         2) conn: an instance of 
          a pg8000:native Connection 
          object.
         
         3) after_time: a time stamp 
          of the form 
          "2025-06-04T08:28:12". If 
          the ToteSys database has 
          changed a row of the table
          after this time, this function 
          considers the row updated.
          On the very first run of 
          pipeline the value of 
          after_time represents 
          1January1900. On subsequent 
          runs its value represents 
          the time of the previous 
          run of the first lambda 
          handler.
          

    Returns:
         a dictionary that looks 
         like this: {
                    "sales_order": [
                        {"Name": "xx", "Month": "January", "Value": 123.45, etc}, <-- one updated row
                        {"Name": "yy", "Month": "January", "Value": 223.45, etc}, <-- one updated row
                        {"Name": "zz", "Month": "January", "Value": 323.45, etc}, <-- one updated row
                                        etc
                                         ] 
                    }
        where "sales_order" is the 
        table name and keys "Name", 
        "Month", "Value", etc are 
        the table's column names. 
        The values of those keys 
        are the row-cell values.

    """


    # Make a list of the column 
    # names of the table in 
    # question: 
    query_result_2 = get_column_names(conn, table_name) # [['staff_id'], ['first_name'], ['last_name'], etc]

    # Get only those rows from 
    # the table that contain 
    # updated data: 
    query_result_1 = get_updated_rows( # [['aaa', 'bbb', 'ccc'], ['ddd', 'eee', 'fff'], etc] 
        conn, 
        after_time, 
        table_name)      


    # Convert query_result_2 to a 
    # list of column-name strings: 
    clean_col_names = [col[0] for col in query_result_2] # ['name', 'location', etc]


    # convert cell values in the 
    # updated rows llike this: 
    # datetime.datetime object -> ISO string
    # Decimal value            -> float
    # json                     -> string:
    cleaned_rows = convert_values(query_result_1) 

    # Make a dictionary for each
    # updated row where the 
    # key-value pairs of each
    # dictionary represent 
    # <column-name>: <cell-value>:
    row_list_of_dicts = make_row_dicts(clean_col_names, cleaned_rows)
    # row_data looks like this:
    # [ ... 
    #   {"design_id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00.123456', etc},         
    #   {"design_id": 7,  "name": "bbb",  "value": 3.15,  "date": '2024-06-01T10:30:00.123456', etc},
    #   etc ]


    return {table_name: row_list_of_dicts}






#=====================
# To print table values: 

lookup = {}
lookup['conn'] = conn_to_db(os.environ['OLTP_NAME'])    # A string
lookup['close_db'] = close_db 


conn = lookup['conn'] # pg8000.native Connection object
close_db = lookup['close_db'] # function to close connection to database

# after_time = "2025-06-04T08:28:12"
after_time = "2000-06-04T08:28:12"

# dict = read_table('design', conn, after_time)
# dict = read_table('address', conn, after_time)
# dict = read_table('sales_order', conn, after_time)
# dict = read_table('staff', conn, after_time)
dict = read_table('currency', conn, after_time)
# dict = read_table('counterparty', conn, after_time)
# dict = read_table('department', conn, after_time)
# close_db(conn)

# print(f'The design table: \n{dict}') 
# print(f'The address table: \n{dict}') 
# print(f'The sales_order table: \n{dict}') 
print(f'The currency table: \n{dict}') 
# print(f'The counterparty table: \n{dict}') 
# print(f'The department table: \n{dict}') 
#=====================


# To get columns names:
# lookup = {}
# lookup['conn'] = conn_to_db(os.environ['OLTP_NAME'])    # A string
# lookup['close_db'] = close_db 

# conn = lookup['conn'] # pg8000.native Connection object
# close_db = lookup['close_db'] # function to close connection to database

# table_name = 'design'
# query_result_2 = get_column_names(conn, table_name)
# clean_col_names = [col[0] for col in query_result_2]
# print(f'Cols for table {table_name} >>>  {clean_col_names}')
# close_db(conn)
















    # Each table has a column labelled 'last_updated'.
    # query_result_1 will be, eg:
    # [ 
    # [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), datetime.datetime(2025, 6, 4, 8, 58, 10, 6000)],
    # [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), datetime.datetime(2025, 6, 4, 9, 26, 9, 972000)],
    # [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), datetime.datetime(2025, 6, 4, 9, 29, 10, 166000)], 
    # etc  
    # ]

    # query_result_2 will be, eg, :
    # [
    #     ['design_id'],
    #     ['created_at'],
    #     ['design_name'],
    #     ['file_location'],
    #     ['file_name'],
    #     ['last_updated'],
    # ] 
    # (because that is what 
    # pg8000.native.Connection returns – other 
    # versions of pg8000 return tuples and can be 
    # made to return dictionaries)


