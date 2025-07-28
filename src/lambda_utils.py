from src.utils_write_to_ingestion_bucket import create_formatted_timestamp
import json
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import logging


logger = logging.getLogger("MyLogger")

load_dotenv()
# bucket_name = os.environ["bucket_name"]


def get_data_from_db(tables, after_time, conn, read_table, convert_data):
    """
    This function:
        1) loops through a list of names of all
            tables in the ToteSys database
        2) calls read_table() for each table 
            to get rows data from the ToteSys 
            database, but only if the data has 
            been updated after a passed-in moment
            in time (given by timestamp string
            after_time)
        3) calls convert_data() for each table
            to convert the row data into a 
            jsonified python list of dictionaries
        4) appends the jsonified python list of
            dicts to a separate list, which ends(thus
            creating a list of jsonified python
                lists (plural) of dictionaries)
            up looking like this: 
            [[{<row data>}, {<row data>}, etc],
            [{<row data>}, {<row data>}, etc],
            etc ]
            CHECK THIS CHECK THIS
            CHECK THIS CHECK THIS  

    Args:
        tables: a list of strings, each string
                being the name of a table in the
                ToteSys database, eg 'sales'.
        after_time: for each table in the ToteSys 
                database this function gets rows
                that contain data that have been 
                modified after after_time. 
        conn: an instance of pg8000's Connection object
        read_table: a utility function that this function
                employs to connect to the ToteSys database
                to get updated data
        convert_data: a utility function that this function
                employs to convert row data that this
                function receives from the ToteSys database
                into a jsonified python list of dictionaries
                (where each dictionary contains data for one
                row of a table).

    Returns:
        a python list each of whose members is a jsonified
                python list of dictionaries

    """
    # read_table() returns, eg, {'sales': [{<data from one row>}, {<data from one row>}, etc]}

    data_list = []
    for table in tables:
        result = read_table(table, conn, after_time)  # {'design': [{<data from one row>}, {<data from one row>}, etc]}
        jsonified_data = convert_data( result )  # jsonified version of {'design': [{<data from one row>}, {<data from one row>}, etc]}
        data_list.append(jsonified_data)
        # data_list is a python list. Each member of that list is a jsonified
        # version of this type of dictionary: 
        # {'<table_name_here>': [{<data from one row>}, {<data from one row>}, etc]}.
        # The dictionary has one key, the name of a table. 
        # The value of that key is a list of dictionaries, each dictionary representing
        # a row of a table. The keys of that dictionary are column names and their values 
        # are the values in that row.
    return data_list






def write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
    """
    This function:
        1) loops through the passed-in python list,
            each of whose members is a jsonified 
            python dictionary that represents  
            updated rows of one table
        2) gets the name of the table from each 
            jsonified dictionary 
        3) looks in the ingestion bucket to determine
            whether one or more keys exist with
            the name of the table at the beginning
            (ie one or more keys beginning with,
            for example, 'sales/'), then
            i) if no such key exists, this is 
             the first ever run of the first 
             lambda function and the table in 
             question has not yet been saved in the 
             ingestion bucket. Also the jsonified 
             python dictionary contains data 
             from every row of the table in
             question. This function then creates
             an appropriate key
             (eg 'design/<*timestamp-here*>.json')
             and saves the whole table to the
             S3 ingestion bucket
            ii) if one or more such keys exist this 
             means the table already exists in the
             ingestion bucket, so this function creates
             a new table that contains all rows, 
             including the updated ones, and saves 
             the new table to the S3 ingestion
             bucket with a new timestamp.
             CHECK THIS CHECK THIS
             CHECK THIS CHECK THIS

    Args:
        1) data_list: a python list each of whose members
            is a jsonified version of this type of 
            dictionary:
            {'design': [{<data from one row>}, 
                {<data from next row>}, etc] }
            Each member of the list relates to one table.
        2) s3_client: the boto3 client.
        3) write_to_ingestion_bucket: a utility function
            that does the actual writing of rows 
            of a table to the S3 ingestion bucket.
        4) bucket_name:  name of the S3 ingestion bucket    

    Returns:
        None

    """

    for member in data_list:  # member is jsonified version of, eg, {'design': [{<data from one row>}, {<data from one row>}, etc]}
        table_data = json.loads( member )  # {'design': [{<data from one row>}, {<data from one row>}, etc]}
        table_name = list(table_data.keys())[0]  # 'design'
        json_data = json.dumps(table_data[table_name]) # jsonified version of [{<data from one row>}, {<data from one row>}, etc]

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
