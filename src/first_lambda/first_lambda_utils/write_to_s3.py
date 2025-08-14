from first_lambda_utils.write_to_ingestion_bucket import create_formatted_timestamp
from botocore.exceptions import ClientError
from dotenv import load_dotenv


import json
import logging


logger = logging.getLogger("MyLogger")

load_dotenv()





def write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name: str):
    """
    This function:
        1) loops through the passed-in python list,
            each of whose members is a dictionary 
            that represents only the updated rows 
            of one table.
        2) gets the name of the table from each 
            dictionary.
        3) looks in the ingestion bucket to determine
            whether one or more keys exist with
            the name of the table at the beginning
            (eg 'transactions/'), then
            i) if no such key exists that means this 
             is the first ever run of the first 
             lambda function and the table in 
             question has not yet been saved in the 
             ingestion bucket (meaning that the
             dictionary contains data from every
             row of the table). This function then 
             creates an appropriate key
             (eg 'design/<*timestamp-here*>')
             and saves the whole table to the
             S3 ingestion bucket
            ii) if one or more such keys exist this 
             means the table already exists in the
             ingestion bucket, so this function reads
             and copies the latest table, updates its 
             outdated rows and saves it as a new table
             in the bucket with a new timestamp.

    Args:
        1) data_list: a python list that looks like this:
            [
                {
                'design': [ {<data from one updated row>}, 
                            {<data from next updated row>}, 
                            etc
                          ] 
                },
                {
                'transactions': [ {<data from one updated row>}, 
                                  {<data from next updated row>}, 
                                  etc
                                ] 
                },
                etc
            ]
            Each member dictionary of the list relates 
            to one table.
            On the very first run of the first lambda 
            function the list contains all tables and all 
            of the rows for each table.
            On subsequent runs of the first lambda 
            function the list is more likely not to 
            contain all tables and each table will most 
            likely contain only some of its rows (the 
            updated ones).
        2) s3_client: the boto3 client.
        3) write_to_ingestion_bucket: a utility function
            that does the actual writing of rows 
            of a table to the S3 ingestion bucket.
        4) bucket_name:  name of the S3 ingestion bucket.

    Returns:
        None

    """

    # Make timestamp that has this form:
    # '2025-06-13_13-13-13'
    timestamp = create_formatted_timestamp()

    for member in data_list:  # member looks like this: {'design': [{<data from one updated row>}, {<data from one updated row>}, etc]}
        table_name = list(member.keys())[0]  # 'design'
        json_data = json.dumps(member[table_name]) # jsonified version of [{<data from one row>}, {<data from one row>}, etc]

        try:
            # find out whether S3 bucket contains any objects 
            # under key table_name:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_name)

        except ClientError as e:
            raise RuntimeError("Error occurred in attempt to read data from the ingestion bucket") from e

        # if yes, write the table data to S3:    
        if response["KeyCount"] > 0:
            try:
                write_to_ingestion_bucket(member[table_name], bucket_name, table_name, s3_client)
            except RuntimeError as e:
                raise RuntimeError from e
            
            # if no, this is the first run of the 
            # first lambda function, so make a new 
            # key and write the table data to the
            # ingestion bucket under that key:
        else:
            try: 
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{table_name}/{timestamp}.json", # 'sales_order/2025-06-11_13-27-29.json'
                    Body=json_data,
                )
            except ClientError as e:
                raise RuntimeError("Error occurred in attempt to write a table to the ingestion bucket.") from e
            
            
