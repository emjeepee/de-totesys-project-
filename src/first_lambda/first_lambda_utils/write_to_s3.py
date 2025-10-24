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
        1) Either: 
            i) Saves data from each member of 
            data_list to the ingestion bucket if 
            this is the first ever run of the 
            first lambda function as that would 
            mean the member contains a whole 
            table read from the ToteSys 
            database.
            or
            ii) passes each member of data_list to 
            function write_to_ingestion_bucket()
            if this is the 2nd-plus run of the 
            first lambda function, as this would 
            mean the member contains only the 
            updated rows of a table, not 
            necessarily all of them.
            write_to_ingestion_bucket() updates
            the rows of an existing table in
            the ingestion bucket.
        2) performs i) and ii) above by looping 
            through the passed-in python list,
            each of whose members is a dictionary 
            that contains either a list of 
            updated rows of one table or all of 
            the rows of a table that has not been 
            updated, and gets the name of the 
            table from each dictionary.
        3) looks in the ingestion bucket to determine
            whether one or more keys exist with
            the name of the table at the beginning
            (eg 'transactions'), then
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
             S3 ingestion bucket under that key.
            ii) if one or more such keys exist this 
             means that this is the 2nd-plus run of
             the first lambda function, so versions 
             of the table already exists in the
             ingestion bucket. This this function 
             then reads and copies the latest table, 
             updates its outdated rows and saves it 
             in the bucket as a new version of the 
             table with a new timestamp.

    Args:
        1) data_list: a python list that looks like this:
            [
                {
                'design': [ {<data from one row>}, 
                            {<data from next row>}, 
                            etc
                          ] 
                },
                {
                'transactions': [ {<data from one row>}, 
                                  {<data from next row>}, 
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
            function the list is more likely to 
            contain fewer than all of the tables and 
            each table will most likely contain only 
            some of its rows (as only some are likely to
            have been updated in the ToteSys database).
        2) s3_client: the boto3 client.
        3) write_to_ingestion_bucket: a utility function.
            If a member dictionary of data_list represents
            only updated rows of a table this function 
            calls write_to_ingestion_bucket() to replace 
            the outdated rows of a table in the ingestion 
            bucket with the updated rows in the member 
            dictionary, saving the new updated table under 
            a new key and leaving the previous table that 
            has the outdated rows in the bucket.
        4) bucket_name: name of the S3 ingestion bucket.

    Returns:
        None

    """

    err_msg_1 = f"There has been an error in function write_to_s3(). \n Unable to read ingestion bucket." 
    err_msg_2 = f"There has been an error in function write_to_s3(). \n Unable to write to ingestion bucket." 

    # Make timestamp that has this form:
    # '2025-06-13_13-13-13'
    timestamp = create_formatted_timestamp()

    for member in data_list:  # typical member: {'design': [{<data from one updated row>}, {<data from one updated row>}, etc]}
        table_name = list(member.keys())[0]  # 'design'

        try:
            # get keys for all objects saved 
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
        # such keys (remember that the key 
        # in its entirety looks like this: 
        # 'design/2025-05-28_15-45-03.json').
        # If yes, then member in data_list
        # represents only the updated rows 
        # of a table, so pass member's 
        # data to function 
        # write_to_ingestion_bucket(),
        # which will update the appropriate 
        # table in the ingestion bucket:    
        if response["KeyCount"] > 0:
            # value of response["KeyCount"]
            # is 0 if no keys match.

            # member[table_name] is [{...}, {...}, {...}, etc]
            write_to_ingestion_bucket(member[table_name], bucket_name, table_name, s3_client)

            
            # if no, then this is the very 
            # first run of the first lambda 
            # function and data_list is a 
            # whole table, so make a new 
            # key and write the table data 
            # to the ingestion bucket under 
            # that key:
        else:
            try: 
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{table_name}/{timestamp}.json", # 'sales_order/2025-06-11_13-27-29.json'
                    Body=json.dumps(member[table_name]) # jsonified [{<data from one updated row>}, etc]
                )
            except ClientError:
                logger.error(err_msg_2)
                raise 
            
            
