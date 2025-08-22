import pytest

from src.third_lambda.third_lambda_utils.third_lambda_init import third_lambda_init






from unittest.mock import patch, Mock

from datetime import datetime, timedelta

from src.third_lambda.third_lambda_utils.third_lambda_init import third_lambda_init



@pytest.fixture
def general_setup():
    # object_key = event["Records"][0]["s3"]["object"]["key"]
    # make mock event:
    mock_event = {
                    'Records': [
                    {"s3": 
                     { "object": {
                         'key': "design/2025-06-13_13:23.parquet" 
                                 }
                     }
                    }
                              ]
                }

    # proc_bucket = lookup['proc_bucket'] # name of processed bucket
    # s3_client = lookup['s3_client']     # boto3 S3 client object
    # object_key = lookup['object_key']   # key under which processed bucket saved Parquet file
    # table_name = lookup['table_name']   # name of table
    # conn = lookup['conn']               # pg8000.native Connection object that knows about warehouse
    # close_db = lookup['close_db']       # function to close connection to warehouse


    yield 





# @pytest.mark.skip
def test_xxx(general_setup):
    # Arrange:


    # Act:


    #Assert:
    assert True




# @pytest.mark.skip
def test_xxx(general_setup):
    # Arrange:


    # Act:


    #Assert:
    assert True





# @pytest.mark.skip
def test_xxx(general_setup):
    # Arrange:


    # Act:


    #Assert:
    assert True
