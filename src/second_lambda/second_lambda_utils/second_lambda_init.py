

def second_lambda_init(event, s3_client, dt_now, dt_start, num_rows):
    """
    This function:
        creates a dictionary whose keys have values 
        that the second lambda function requires.    
    
    Args:
        event: the event object passed as argument 
         by Lambda to the second lambda handler.
         An example is:
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
    #                         },
    #         "requestParameters": {
    #             "sourceIPAddress": "127.0.0.1"
    #                              },
    #         "responseElements": {
    #             "x-amz-request-id": "EXAMPLE123456789",
    #             "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
    #                              },
    #         "s3": {
    #             "s3SchemaVersion": "1.0",
    #             "configurationId": "testConfigRule",
    #             "bucket": {
    #                 "name": "11-ingestion-bucket",
    #                 "ownerIdentity": {
    #                     "principalId": "EXAMPLE"
    #                                  },
    #                 "arn": "arn:aws:s3:::example-bucket"
    #                       },
    #             "object": {
    #                 "key": "design/2025-06-13_13:23:34", # the key under which the object has been saved 
    #                 "size": 1024,
    #                 "eTag": "0123456789abcdef0123456789abcdef",
    #                 "sequencer": "0A1B2C3D4E5F678901"
    #                       }
    #               }
    #     }
    #     ]
    # }
        s3_client:
            a boto3 S3 client object.
        dt_now: 
            a datetime object representing the 
            current time.
        dt_start:
            a datetime object representing the             
            date from which to create a 
            date dimension table (ie the date of
            the first row).
        num_rows:
            the number of rows that the date
            dimension table will have.    

    Returns:
        a dictionary containing the values
        the second lambda requires
    """

    # Get the key under which 
    # the ingestion bucket has 
    # stored the table: 
    object_key = event["Records"][0]["s3"]["object"]["key"]

    lookup = {
        's3_client': s3_client, # boto3 S3 client object
        'timestamp_string' : dt_now.strftime("%Y-%m-%d_%H-%M-%S"), # string of format "2025-08-14_12-33-27"
        'ingestion_bucket': event["Records"][0]["s3"]["bucket"]["name"], # name of ingestion bucket   
        'object_key': object_key, # # key for object in ingestion bucket, eg sales_order/2025-06-04_09-21-32.json
        'proc_bucket': "11-processed-bucket", # name of processed bucket:
        'table_name': object_key.split("/")[0], # name of table, eg 'sales_order'
        'start_date': dt_start, # datetime object for 1 Jan 2024 (includes time info for midnight)
        'num_rows' : num_rows # a datetime object for 1 Jan 2024
            }

    return lookup