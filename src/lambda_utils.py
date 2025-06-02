from src.conn_to_db import conn_to_db
from src.utils import convert_data, read_table
from src.utils_write_to_ingestion_bucket import create_formatted_timestamp
import os
import json
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()
bucket_name = os.environ["bucket_name"]
def get_data_from_db(tables, after_time, conn, read_table,convert_data):
    data_list = []
    for table in tables:
        result = read_table(table, conn, after_time)
        prefix = next(iter(result))
        data = result[prefix]
        jsonified_data = convert_data(result)
        data_list.append(jsonified_data)
        # data_list is python list each of who's members is a jsonified python list of dictionaries
    return data_list

def write_to_s3(data_list,s3_client, write_to_ingestion_bucket):
    for i in len(data_list):
        prefix = list(json.loads(i).keys())[data_list.index(i)]
        print(prefix)
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if response["KeyCount"] > 0:
                write_to_ingestion_bucket(json.loads(data_list[i]), bucket_name, prefix)
            else:
                timestamped = create_formatted_timestamp()
                s3_client.put_object(Bucket=bucket_name, Key=f"{prefix}/{timestamped}.json", Body=json.dumps(data_list[i]))

        except ClientError as e:
            print(e)


