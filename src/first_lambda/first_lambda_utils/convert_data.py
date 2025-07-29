import json
from src.first_lambda.first_lambda_utils.serialise_datetime import serialise_datetime



def convert_data(data: dict):
    """
    This function:
        converts the data passed in into json format.

    Args:
        data: a dict 
        
    Returns:
        A json string, ready to upload into the 
        ingestion S3 bucket as a json file
    """

    # If the value of a key in dict data is 
    # a datetime.datetime object or a 
    # decimal.Decimal object then this function
    # uses helper function serialise_datetime to 
    # convert the value to a string:
    return json.dumps(data, default=serialise_datetime)
