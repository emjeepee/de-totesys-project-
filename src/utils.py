import json
from pg8000.native import Connection
import datetime
import decimal
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
    # variable result will contain a 
    # list of tuples, each of which 
    # represents a row, eg:
    # [('xx', "aaa", 3.14, etc)
    #  ('yy', "bbb", 3.15, etc)
    #  ('zz', "ccc", 3.16, etc) ]
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
    # of tuples, each tuple containing a
    # a string for the name of a column
    # of the table in question:
    query_result = conn.run(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
                           )

    # Convert query_result to a list 
    # of column-name strings: 
    column_names = [col[0] for col in query_result] # ['name', 'location', etc]



    dict_data = []

    for i in result:
        if isinstance(i, tuple):
            i = list(i) # eg [7, "bbb", 3.15, etc]
        if isinstance(i, list):
            dic_to_append = {}
            for j in range(len(i)):
                if isinstance(i[j], datetime.datetime):
                    dic_to_append[column_names[j]] = i[j].isoformat()
                else:
                    dic_to_append[column_names[j]] = i[j]
            # dict_to_append now looks like {"id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00', etc},         
            # ie contains columns names and row-cell values for one row of the table in question.
            dict_data.append(dic_to_append)
            # dict_data ends up looking like [ {"id": 6,  "name": "aaa",  "value": 3.14,  "date": '2024-05-01T10:30:00', etc},         
            #                           {"id": 7,  "name": "bbb",  "value": 3.15,  "date": '2024-05-01T10:30:00', etc},
            #                           etc ]
            # ie it's a list that contains dicts. each dict
            # represents a row of updated data and each 
            # key-value pair in a dict represents <column name>:<cell value>.

    return {table_name: dict_data}








def convert_data(data: dict | list):
    """
    This function:
        converts the data passed in into json format.

    Args:
        data, a dict 
        
    Returns:
        A json string, ready to upload into the 
        ingestion S3 bucket as a json file
    """

    # If the value of a key in dict data is 
    # a datetime.datetime object or a 
    # datetime.date object then this function
    # uses the helper function to convert 
    # the value to a string or float, 
    # respectively:
    try:
        return json.dumps(data, default=serialize_datetime)
    except (ValueError, TypeError) as error:
        logger.error("Unable to dump the data")
        raise ValueError(f"Data cannot be converted: {error}")






def serialize_datetime(obj):
    """
    This function:
        1) is used by json.dumps(), which 
           function convert_data() 
           employs.
        2) converts the passed-in object
           to an ISO date string if it is
           a datetime.datetime object.    
        3) converts the passed-in object
           to a string if it is a
           decimal.Decimal object.    


    Args:
        an object that is either a 
        datetime.datetime object or 
        a decimal.Decimal object.

    Returns:
        a string.
    """
    if isinstance(obj, (datetime.datetime, decimal.Decimal)):
        return obj.isoformat()  # Convert datetime
    if isinstance(obj, decimal.Decimal):
        return str(obj)
