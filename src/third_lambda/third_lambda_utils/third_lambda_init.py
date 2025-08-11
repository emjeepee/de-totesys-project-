import boto3
import datetime



def third_lambda_init(event):
    """
    This function:
        creates a dictionary whose keys have values 
        that the third lambda function requires.    
    
    Args:
        event: the event object passed as argument 
         by Lambda to the third lambda handler.
         An example of event is:
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
    """

    # processed bucket name is "11-processed-bucket"

    # 
    object_key = event["Records"][0]["s3"]["object"]["key"]

    lookup = {
        's3_client': boto3.client("s3"),
        # The key under which the ingestion bucket stores an object: 
        'object_key': object_key, # eg ???????
        # The name of the processed bucket:
        'proc_bucket': ["Records"][0]["s3"]["bucket"]["name"],
        # The name of the table that the object in the
        # ingestion bucket holds:
        'table_name': object_key.split("/")[0] # 'sales_order' ??????
                     }

    


    return lookup