import boto3
from src.utils_write_to_ingestion_bucket import (
    write_to_ingestion_bucket,
    create_formatted_timestamp,
)
from src.utils import convert_data, read_table
from datetime import datetime
from botocore.exceptions import ClientError
from src.conn_to_db import conn_to_db, close_db
import json
from pg8000.native import Connection
from src.lambda_utils import get_data_from_db, write_to_s3
from src.utils_write_to_ingestion_bucket import write_to_ingestion_bucket
from src.change_after_time_timestamp import change_after_time_timestamp


# write_to_s3(data_list, s3_client, write_to_ingestion_bucket, bucket_name)


def lambda_handler(event=None, context=None):
    s3_client = boto3.client("s3")
    bucket_name = "11-ingestion-bucket"
    tables = [
        "design",
        "payment",
        "sales",
        "transaction",
        "sales_order",
        "counterparty",
        "address",
        "staff",
        "purchase_order",
        "department",
        "currency",
        "payment_type",
    ]
    conn = conn_to_db("totesys")

    after_time = change_after_time_timestamp(bucket_name, s3_client, "***timestamp***", "1900-01-01 00:00:00")
    
    data_for_s3 = get_data_from_db(tables, after_time, conn, read_table, convert_data)
    write_to_s3(data_for_s3, s3_client, write_to_ingestion_bucket, bucket_name)

    close_db(conn)


# print(lambda_handler("event","context"))
