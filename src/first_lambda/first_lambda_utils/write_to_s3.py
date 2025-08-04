from first_lambda_utils.write_to_ingestion_bucket import create_formatted_timestamp
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import logging


logger = logging.getLogger("MyLogger")

load_dotenv()





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

    # Make timestamp that code will  employ if 
    # 
    timestamp = create_formatted_timestamp()

    for member in data_list:  # member looks like this: {'design': [{<data from one row>}, {<data from one row>}, etc]}
        table_name = list(member.keys())[0]  # 'design'
        json_data = convert_data(member[table_name])

        try:
            # find out whether S3 bucket contains any objects 
            # under key table_name:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_name)

        except ClientError as e:
            raise RuntimeError("Error occurred in attempt to read data from the ingestion bucket") from e

        # if yes, write the table data to S3:    
        if response["KeyCount"] > 0:
            try:
                write_to_ingestion_bucket(json_data, bucket_name, table_name, s3_client)
            except RuntimeError as e:
                raise RuntimeError from e
            
            # if no, make a new timestamp, make the 
            # timestamp part of a key and write the 
            # table data to S3 bucket under that key
            # (this happens on very first run of 
            # first_lambda_handler(), the first 
            # lambda function):
        else:
            try: 
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{table_name}/{timestamp}.json",
                    Body=json_data,
                )
            except ClientError as e:
                raise RuntimeError("Error occurred in attempt to write a table to the ingestion bucket.") from e
