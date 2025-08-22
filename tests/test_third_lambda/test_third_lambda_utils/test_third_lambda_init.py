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
                         'key': "design/2025-06-13_13:23:34" 
                                 }
                     }
                    }
                              ]
                }

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
