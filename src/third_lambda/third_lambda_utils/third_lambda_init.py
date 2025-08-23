import boto3






def third_lambda_init(event, conn_to_db, close_db, s3_client):
    """
    This function:
        creates a dictionary whose keys have values 
        that the third lambda function requires.    
    
    Args:
        1) event: the event object passed as argument 
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
    #                 "name": "11-processed-bucket",
    #                 "ownerIdentity": {
    #                     "principalId": "EXAMPLE"
    #                 },
    #                 "arn": "arn:aws:s3:::example-bucket"
    #             },
    #             "object": {
    #                 "key": "design/2025-06-13_13-23.parquet" OR fact_sales_order/2025-06-13_13-23.parquet, 
    #                                   # the key under which the object has been saved 
    #                 "size": 1024,
    #                 "eTag": "0123456789abcdef0123456789abcdef",
    #                 "sequencer": "0A1B2C3D4E5F678901"
    #             }
    #         }
    #     }
    #     ]
    # }
            2) conn_to_db: utility function that 
                makes a connection to a postgresql
                database.  
            3) close_db: utility function that 
                closes a connection to a postgresql
                database.  
            4) s3_client: the boto3 s3 client 
                object (boto3.client('s3')).

    Returns:
        a dictionary that is a lookup table
        that the third lambda handler employs
        to find values it requires.

    """

    object_key = event["Records"][0]["s3"]["object"]["key"]

    lookup = {
        's3_client': s3_client,                                     # boto3 S3 client
        'object_key': object_key,                                   # key for Parquet file in processed bucket 
        'proc_bucket': event["Records"][0]["s3"]["bucket"]["name"], # name of processed bucket
        'table_name': object_key.split("/")[0],                     # name of Parquet file in processed bucket
        'conn': conn_to_db('WAREHOUSE'),                            # pg8000.native Connection object
        'close_db': close_db                                        # function to close connection to warehouse
             }


    return lookup