from .create_formatted_timestamp import create_formatted_timestamp

from botocore.exceptions import ClientError
from dotenv import load_dotenv

import json
import logging


load_dotenv()



logger = logging.getLogger(__name__)



def write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
    """
    This function:
        deals with two scenarios:
        1) data_list contains all tables and
         all rows of all tables because this 
         is the first ever run of this project.
         This function then saves each table 
         and all of its rows in the ingestion 
         bucket under a key that looks like 
         this: 'design/<timestamp-here>'

        2) data_list contains only those tables
         that the ToteSys database has updated.
         Each of those tables contains only 
         updated rows.
         In this case this function creates a 
         copy of a table that already exists in 
         the ingestion bucket, updates the 
         appropriate rows in the copy and saves 
         the table under a new key that looks 
         like this: 'design/<timestamp-here>'.
         The original table in the ingestion 
         bucket remains unchanged.

    Args:
        1) data_list: a list containing tables 
            and their rows. This list contains 
            member dictionaries, each of which 
            represents one table. data_list 
            looks like this, for example:
            [
            {'design': [{row data>}, {row data>}, etc]},   
            {'sales_orders': [{row data>}, {row data>}, etc]},   
            etc
            ].
            where keys 'design', 'sales_orders', etc are the
             names of tables and where 
             {<row data>} is, for example: 
            {
                'design_id': 123,       
                'created_at': 'xxx',    
                'design_name': 'yyy',   
                etc                     
            }
            On the very first run of this project
            data_list contains all tables and all 
            of the rows for each table.
            On subsequent runs data_list is more 
            likely to contain fewer than all of 
            the tables and each table will most 
            likely contain only some of its rows. 
        2) s3_client: the boto3 client.
        3) write_to_ingestion_bucket: a utility 
            function. If the member dictionaries 
            of data_list are updated tables then 
            write_to_s3() calls 
            write_to_ingestion_bucket(), which 
            creates new tables with updated rows 
            and saves them in the ingestion bucket, 
            leaving the previous outdated table
            there too.
        4) bucket_name: name of the S3 ingestion 
            bucket.

    Returns:
        None

    """

    print(f"MY_INFO >>>>> In first lambda util write_to_s3(). data_list should be all fake tables. Actual value of first member is {data_list[0]}")        

    err_msg_1 = f"There has been an error in function write_to_s3(). \n Unable to read ingestion bucket." 
    err_msg_2 = f"There has been an error in function write_to_s3(). \n Unable to write to ingestion bucket." 

    # Make timestamp that has this form:
    # '2025-06-13_13-13-13'
    timestamp = create_formatted_timestamp()


    # data_list is, eg, 
    # [
    # {'design': [{<row data>}, {<row data>}, etc]}, 
    # {'sales_orders': [{<row data>}, {<row data>}, etc]}, 
    # etc
    # ], 
    # where {<row data>>} is, eg, 
    # {
    # 'design_id': 123, 
    # 'created_at': 'xxx', 
    # 'design_name': 'yyy',
    #  etc
    # }
    for member in data_list: # member is, eg, {'design': [{<updated-row data>}, {<updated-row data>}, etc]}
        table_name = list(member.keys())[0]  # 'design'

        try:
            # get keys for all objects  
            # in the ingestion bucket that 
            # have a prefix of table_name
            # (if no keys match then response
            # will be a dict without a 
            # "Contents" key):
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_name)

        except ClientError:
            logger.error(err_msg_1)
            raise 

        # Determine whether there are any
        # such keys (which look like this:
        # 'design/2025-05-28_15-45-03.json').
        # If yes, then member in data_list
        # represents a table with updated 
        # rows, so pass the member's rows
        # and table name to function 
        # write_to_ingestion_bucket(),
        # which will update that table
        # in the ingestion bucket:    
        if response["KeyCount"] > 0:
            # value of response["KeyCount"]
            # is 0 if no keys match.

            # member[table_name] is [{<updated-row data>}, {<updated-row data>}, etc]
            write_to_ingestion_bucket(member[table_name], bucket_name, table_name, s3_client)

            
            # if no, then this is the very 
            # first run of the project
            # and data_list contains every 
            # table and every row of every 
            # table, so make a new key and 
            # write the table data to the 
            # ingestion bucket under that key:
        else:
            try: 
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{table_name}/{timestamp}.json", # 'sales_order/2025-06-11_13-27-29.json'
                    Body=json.dumps(member[table_name]) # jsonified [{<updated-row data>}, {<updated-row data>}, etc]
                )
            except ClientError:
                logger.error(err_msg_2)
                raise 
            
            
