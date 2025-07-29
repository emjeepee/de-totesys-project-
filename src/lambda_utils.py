from src.utils_write_to_ingestion_bucket import create_formatted_timestamp
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import logging


logger = logging.getLogger("MyLogger")

load_dotenv()




def get_data_from_db(table_names: list, after_time: str, conn, read_table):
    """
    This function:
        1) loops through a list of names of all
            tables in the ToteSys database
        2) in the loop calls read_table() for 
            each table name to get updated rows 
            for that table from the ToteSys 
            database.
            'Updated' here means modified in the 
            ToteSys database after passed-in 
            time after_time.
            read_table() returns a dictionary.
        3) appends each dictionary to
            a separate list.
        4) returns that list.            

    Args:
        table_names: a list of strings, each string
                being the name of a table in the
                ToteSys database, eg 'sales'.
        after_time: for each table in the ToteSys 
                database get_data_from_db() gets rows
                that contain data that have been 
                modified after after_time. 
        conn: an instance of pg8000's Connection object
        read_table: a utility that this function
                employs to read the ToteSys database 
                and get a table's updated rows from it.

    Returns:
        a python list each of whose members is a jsonified
                python dictionary that represents a table 
                and that contains a list of the table's 
                updated rows. Each row is in the form of 
                a dictionary.


    """
    # read_table() below returns a dictionary, for example
    # {'sales': [{<data from one row>}, {<data from one row>}, etc]},
    # where {<data from one row>} is an updated row.

    data_list = []
    for table in table_names:
        result = read_table(table, conn, after_time)  # {'design': [{<data from one row>}, {<data from one row>}, etc]}
        # OLD CODE: jsonified_data = convert_data( result )  # jsonified version of result
        data_list.append(result)
        # data_list is a python list. Each member of that list is 
        # this type of dictionary: 
        # {'<table_name_here>': [{<data from one row>}, {<data from one row>}, etc]}.
        # The dictionary has one key, the name of a table. 
        # The value of that key is a list of dictionaries, each dictionary representing
        # an updated row of that table. The keys of that dictionary are column names and 
        # their values are the values in that row.
    return data_list






def write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str, convert_data):
    """
    This function:
        1) loops through the passed-in python list,
            each of whose members is a dictionary 
            that represents updated rows of one table
        2) gets the name of the table from each 
            jsonified dictionary 
        3) looks in the ingestion bucket to determine
            whether one or more keys exist with
            the name of the table at the beginning
            (ie looks for one or more keys beginning with
            'sales/' for example), then
            i) if no such key exists, this is 
             the first ever run of the first 
             lambda function and the table in 
             question has not yet been saved in the 
             ingestion bucket. Also the 
             dictionary contains data 
             from every row of the table in
             question. This function then creates
             an appropriate key
             (eg 'design/<*timestamp-here*>.json')
             and saves the whole table to the
             S3 ingestion bucket
            ii) if one or more such keys exist this 
             means the table already exists in the
             ingestion bucket, so this function copies
             the table, updates its outdated rows 
             and saves it as a new table in the 
             bucket with a new timestamp.

    Args:
        1) data_list: a python list each of whose members
            is a dictionary that looks like this:
            {'design': [{<data from one updated row>}, 
                {<data from next updated row>}, etc] }
            Each member of the list relates to one table.
        2) s3_client: the boto3 client.
        3) write_to_ingestion_bucket: a utility function
            that does the actual writing of rows 
            of a table to the S3 ingestion bucket.
        4) bucket_name:  name of the S3 ingestion bucket.
        5) convert_data: a utility that this function uses 
            to convert table data to json. If the row 
            data from that table includes values that are
            in the Datetime Datetime or Datetime Decimal 
            format, then convert_data() changes them to 
            ISO format before jsonifying them.      

    Returns:
        None

    """

    for member in data_list:  # member looks like this: {'design': [{<data from one row>}, {<data from one row>}, etc]}
        # OLD CODE: table_data = json.loads( member )  # {'design': [{<data from one row>}, {<data from one row>}, etc]}
        table_name = list(member.keys())[0]  # 'design'
        # OLD CODE: json_data = json.dumps(member[table_name]) # jsonified version of [{<data from one row>}, {<data from one row>}, etc]
        json_data = convert_data(member[table_name])

        try:
            # find out whether S3 bucket contains any objects 
            # under key table_name:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_name)
            # if yes, write the table data to S3
            if response["KeyCount"] > 0:
                write_to_ingestion_bucket(json_data, bucket_name, table_name, s3_client)
            # if no, make a new timestamp, make the 
            # timestamp part of a key and write the 
            # table data to S3 bucket under that key:
            else:
                timestamp = create_formatted_timestamp()
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{table_name}/{timestamp}.json",
                    Body=json_data,
                )

        except ClientError as e:
            logger.error("Unable to write to S3")
            return e
