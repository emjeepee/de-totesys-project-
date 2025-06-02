import boto3
from src.utils_write_to_ingestion_bucket import write_to_ingestion_bucket
from src.utils import convert_data, read_table
from datetime import datetime
from botocore.exceptions import ClientError
from src.conn_to_db import conn_to_db, close_db
import json
from pg8000.native import Connection
from src.lambda_utils import get_data_from_db

def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    bucket_name = "11-ingestion-bucket"
    after_time = "1900-01-01 00:00:00"
    tables = ["design", "payment", "sales", "transaction", "sales_order", "counterparty", "address", "staff", "purchase_order", "department", "currency", "payment_type" ]
    conn = conn_to_db("totesys")

    get_data_from_db(tables, after_time, conn, read_table, convert_data)


    #   try:
    #      response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    #      if response["KeyCount"] > 0:
    #         write_to_ingestion_bucket(json.loads(data), bucket_name, prefix)
    #      else:
    #         timestamped = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    #         s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/{timestamped}.json", Body=json.dumps(data))

    #   except ClientError as e:
    #      print(e)


    close_db(conn)


#print(lambda_handler("event","context"))