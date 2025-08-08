import boto3
import datetime


from src.second_lambda.second_lambda_utils.create_and_save_dim_date import create_and_save_dim_date
from src.second_lambda.second_lambda_utils.read_from_s3 import read_from_s3
from src.second_lambda.second_lambda_utils.function_lookup_table import function_lookup_table
from src.second_lambda.second_lambda_utils.convert_to_parquet import convert_to_parquet
from src.second_lambda.second_lambda_utils.upload_to_s3 import upload_to_s3
from src.second_lambda.second_lambda_utils.get_latest_table import get_latest_table






def lambda_handler(event, context):
    """
    This function:
        1) receives an event from the S3 ingestion bucket
            when the first lambda function has saved an 
            object in that bucket. The object represents 
            a table and contains all of the rows of that 
            table. 
        2) reads passed-in event to get the name of the S3 
            ingestion bucket and the name of the key under 
            which the first lambda stored an object in 
            that bucket.
        3) creates a date dimension table if this is the 
            first ever run of this lambda function, 
            converts the date dimension table to parquet 
            form and saves it to the processed bucket
        4) gets the table stored under the key, converts 
            it to a dimension table (in the form of a 
            Python list), converts that to a Parquet file
            and stores the Parquet file in the processed 
            S3 bucket under a new key, part of which is
            a timestamp for the current time

    """


    # example event:
    # {
    #     "Records": [
    #     {
    #         "eventVersion": "2.0",
    #         "eventSource": "aws:s3",
    #         "awsRegion": "us-east-1",
    #         "eventTime": "1970-01-01T00:00:00.000Z",
    #         "eventName": "ObjectCreated:Put",
    #         "userIdentity": {
    #             "principalId": "EXAMPLE"
    #         },
    #         "requestParameters": {
    #             "sourceIPAddress": "127.0.0.1"
    #         },
    #         "responseElements": {
    #             "x-amz-request-id": "EXAMPLE123456789",
    #             "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
    #         },
    #         "s3": {
    #             "s3SchemaVersion": "1.0",
    #             "configurationId": "testConfigRule",
    #             "bucket": {
    #                 "name": "ingestion-bucket",
    #                 "ownerIdentity": {
    #                     "principalId": "EXAMPLE"
    #                 },
    #                 "arn": "arn:aws:s3:::example-bucket"
    #             },
    #             "object": {
    #                 "key": "design/2025-06-13_13:23:34", # the key under which the object has been saved 
    #                 "size": 1024,
    #                 "eTag": "0123456789abcdef0123456789abcdef",
    #                 "sequencer": "0A1B2C3D4E5F678901"
    #             }
    #         }
    #     }
    #     ]
    # }


    s3_client = boto3.client("s3")
    dt_timestamp = datetime.datetime.now().isoformat().replace(":", "-") # staff/2025-07-08T13-13-13.123456.parquet
    ingestion_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]     # sales_order/2025-06-04_09-21-32.json
    proc_bucket = "11-processed-bucket"
    # Get table name and timestamp
    # from the key of the object:
    split_key = object_key.split("/") # ['sales_order', '2025-06-04_09-21-32.json']
    table_name = split_key[0] # 'sales_order'
    timestamp = split_key[1] # '2025-06-04_09-21-32.json'



    # Get the jsonified python list that
    # is the table that this function has 
    # just been notified about. The first 
    # lambda function just saved that 
    # table in the ingestion bucket:
    table_json = read_from_s3(s3_client, ingestion_bucket, object_key)
    # jsonified[{...}, {...}, {...}]

    # convert the jsonified table into
    # a python version:
    table_python = json.loads(table_json) # [{...}, {...}, {...}]

    # Make the string that will be the key 
    # under which this second lambda function 
    # will save the dimension/fact table to the 
    # processed bucket (note that when you put 
    # a datetime object in an fstring, Python 
    # converts the object to a string!):
    processed_key = f"{dt_timestamp}/{table_name}.parquet"

    # If this is the first ever run of the ETL 
    # pipeline make a date dimension table, 
    # convert it to parquet form and save it
    # in the processed bucket:
    create_and_save_dim_date(s3_client, proc_bucket, dt_timestamp)


    # Run the appropriate function to make 
    # either the facts table or a dimension table:
    # Get the latest department table:

        # Note the choice of 'dim_or_facts_table' 
        # is to reduce lines of code. 
        # dim_or_facts_table as set in the two if 
        # statements below will be a dimension 
        # table only:    
    if table_name == 'staff':
        # get latest department table in ingestion bucket:
        dept_python = get_latest_table(s3_client, ingestion_bucket, 'department')
        dim_or_facts_table = function_lookup_table[table_name](table_python, dept_python)    
        
    if table_name == 'counterparty':
        # get latest address table in ingestion bucket:
        address_python = get_latest_table(s3_client, ingestion_bucket, 'address')
        dim_or_facts_table = function_lookup_table[table_name](table_python, address_python)    
 
    # dim_or_facts_table below can be either a 
    # dimension table or the facts table:
    dim_or_facts_table = function_lookup_table[table_name](table_python)




    # Convert the dim/facts table to Parquet form:
    pq_file = convert_to_parquet(dim_or_facts_table)

    # Save the Parquet file to the processed bucket
    # under an appropriate key:
    # key will be "folder/myfile.parquet
    upload_to_s3(s3_client, proc_bucket, processed_key, pq_file)
















# OLD CODE:
    # if table_name == "sales_order":
    #     processed_python = transform_to_star_schema_fact_table(
    #         table_name, ingestion_python
    #     )

    # elif table_name == "staff":
    #     # get data from the department table with the same timestamped file name
    #     dept_json = read_from_s3(
    #         s3_client, ingestion_bucket, f"department/{timestamp}"
    #     )
    #     dept_python = convert_json_to_python(dept_json)
    #     processed_python = transform_to_dim_staff(ingestion_python, dept_python)

    # elif table_name == "address":
    #     processed_python = transform_to_dim_location(ingestion_python)

    # elif table_name == "design":
    #     processed_python = transform_to_dim_design(ingestion_python)

    # elif table_name == "counterparty":
    #     # get data from the address table with the same timestamped file name
    #     address_json = read_from_s3(
    #         s3_client, ingestion_bucket, f"address/{timestamp}"
    #     )
    #     address_python = convert_json_to_python(address_json)
    #     processed_python = transform_to_dim_counterparty(
    #         ingestion_python, address_python
    #     )

    # elif table_name == "currency":
    #     processed_python = transform_to_dim_currency(ingestion_python)




    # dept_json = read_from_s3(
    #         s3_client, ingestion_bucket, f"department/{timestamp}"
    #                         )
    # dept_python = json.load(dept_json)
    # # Get the latest address table:
    # address_json = read_from_s3(
    #         s3_client, ingestion_bucket, f"address/{timestamp}"
    #                         )
    # address_python = json.load(address_json)
