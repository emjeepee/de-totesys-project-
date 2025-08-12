import json

from src.second_lambda.second_lambda_utils.create_dim_date_Parquet  import create_dim_date_Parquet
from src.second_lambda.second_lambda_utils.read_from_s3             import read_from_s3
from src.second_lambda.second_lambda_utils.convert_to_parquet       import convert_to_parquet
from src.second_lambda.second_lambda_utils.upload_to_s3             import upload_to_s3
from src.second_lambda.second_lambda_utils.second_lambda_init       import second_lambda_init
from src.second_lambda.second_lambda_utils.make_dim_or_fact_table   import make_dim_or_fact_table
from src.second_lambda.second_lambda_utils.is_first_run_of_pipeline import is_first_run_of_pipeline





def lambda_handler(event, context):
    """
    This function:
        1) receives an event from the S3 ingestion bucket
            when the first lambda function has saved a 
            table in that bucket. The table includes 
            all rows. The ingestion bucket saved the
            table as a jsonified list of dictionaries
            where each dictionary represents a row of
            the table. 
        2) reads passed-in event to get the name of the S3 
            ingestion bucket and the name of the key under 
            which the first lambda stored the table in 
            that bucket.
        3) gets the table stored under the key, converts 
            it to a dimension table or fact table (in the 
            form of a Python list), converts that to a 
            Parquet file and stores the Parquet file in 
            the processed S3 bucket under new key 
            f"{timestamp}/fact_{table_name}.parquet" or
            f"{timestamp}/dim_{table_name}.parquet".
        4) creates a date dimension table if this is the 
            first ever run of this lambda function, 
            converts that table to Parquet form and saves
            it to the processed bucket.

    Args:
        event: the event object sent to this lambda 
         handler by AWS EventBridge in response to the 
         first lambda handler storing a table in the 
         ingestion bucket.

    Returns:
        None                    
    """

    # Get lookup table that contains 
    # values this lambda handler requires:
    lookup = second_lambda_init(event)
    # Set variables to values held by
    # the lookup table: 
    # 1) the boto3 S3 client object,
    # 2) the name of the ingestion bucket,
    # 3) the key under which the ingestion
    #    bucket stores the object (ie table),
    # 4) a datetime timestamp for the 
    #    current time,
    # 5) the name of the table and
    # 6) the name of the processed bucket:
    s3_client = lookup['s3_client']
    ingestion_bucket = lookup['ingestion_bucket']
    object_key = lookup['object_key']
    timestamp_string = lookup['timestamp_string'] # time now, eg "2025-08-14_12-33-27"
    table_name = lookup['table_name']
    proc_bucket = lookup['proc_bucket']
    start_date = lookup['start_date'] # a datetime object for 1 Jan 2024

    # Get the jsonified python list that
    # is the table that this function has 
    # just been notified about and 
    # convert it to a python list:
    table_json = read_from_s3(s3_client, ingestion_bucket, object_key) # jsonified[{...}, {...}, {...}]
    table_python = json.loads(table_json) # [{...}, {...}, {...}]

    # If this is the first ever run of the ETL 
    # pipeline (ie if the processed bucket is 
    # empty) make a date dimension table in 
    # Parquet form and save it in the 
    # processed bucket: 
    if is_first_run_of_pipeline(proc_bucket, s3_client):
        arr = create_dim_date_Parquet(start_date, timestamp_string)
        upload_to_s3(s3_client, proc_bucket, arr[1], arr[0])


    # Make either the fact table or a dimension 
    # table (whichever is appropriate) as a 
    # Python list of dictionaries:
    dim_or_fact_table = make_dim_or_fact_table(table_name, table_python, s3_client, ingestion_bucket)

    # Convert the dim/fact table to Parquet form. 
    # This preserves the order of the keys as they 
    # were in the dictionaries (important for the 
    # utility function of the third lambda handler 
    # that makes SQL query strings):
    pq_file = convert_to_parquet(dim_or_fact_table)

    # Make the key (a string) under which this
    # second lambda function will save the 
    # dimension/fact table in the processed 
    # bucket (note that when you put 
    # a datetime object in an fstring, Python 
    # converts the object to a string!):
    table_key = f"{timestamp_string}/fact_{table_name}.parquet" if table_name == "sales_order" else f"{timestamp_string}/dim_{table_name}.parquet"
    
    # Save the Parquet file in the processed 
    # bucket:
    upload_to_s3(s3_client, proc_bucket, table_key, pq_file)
















# OLD CODE:




    # # Put this block of code in a 
    # # separate utility function called
    # # init(event, ) 
    # # that returns all of the vars shown below
    # # (except timestamp if it is never used): 
    # s3_client = boto3.client("s3")
    # dt_timestamp = datetime.datetime.now().isoformat().replace(":", "-") # 2025-07-08T13-13-13.123456
    # ingestion_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    # object_key = event["Records"][0]["s3"]["object"]["key"]     # sales_order/2025-06-04_09-21-32.json
    # proc_bucket = "11-processed-bucket"
    # # Get table name and timestamp
    # # from the key of the object:
    # split_key = object_key.split("/") # ['sales_order', '2025-06-04_09-21-32.json']
    # table_name = split_key[0] # 'sales_order'
    # timestamp = split_key[1] # '2025-06-04_09-21-32.json'





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
