import pytest
import json
import logging


from botocore.exceptions import ClientError
from unittest.mock import Mock, patch, ANY

from src.first_lambda.first_lambda_utils.get_latest_table import get_latest_table
from src.first_lambda.first_lambda_utils.errors_lookup import errors_lookup



@pytest.fixture
def test_list():
    # get_latest_table(resp_dict, S3_client: boto3.client, bucket_name: str):
    mock_resp_dict = {
            "Contents": [
                {"Key": 'design/2025-06-02T22-17-19-2513.json'},
                {"Key": 'design/2025-05-29T22-17-19-2513.json'},
                {"Key": 'design/2025-04-29T22-17-19-2513.json'},
                        ]
                    }

    # Make mock S3 client:
    mock_s3_client = Mock()

    # Mock what get_latest_table 
    # should return:
    mock_table_data = { 'design': [{"design_id": 1, "xxx": "aaa"}, {"design_id": 2, "yyy": "bbb"}, {"design_id": 3, "zzz": "ccc"}] }

    mock_bucket_name = "mock_ingestion-bucket"

    # Need to mock these lines in get_latest_table:
    # response = S3_client.get_object(Bucket=bucket_name, Key=latest_table_key)
    # data = response["Body"].read().decode("utf-8")
    # mock the get_object() function's return value.
    # The return value must be a dictionary with 
    # key "Body" and value a read() method that 
    # returns jsonified mock table data encoded as 
    # utf-8:
    mock_s3_client.get_object.return_value = {
        "Body": Mock(read=lambda: json.dumps(mock_table_data).encode("utf-8"))
                                             }
       

    mock_values = [mock_resp_dict, mock_s3_client, mock_bucket_name, mock_table_data]

    return mock_values




def test_get_latest_table_returns_list(test_list):
    # Arrange: 
    # get_latest_table(resp_dict, S3_client: boto3.client, bucket_name: str):

    # Act
    result = get_latest_table(test_list[0], test_list[1], test_list[2])

    # Assert
    assert isinstance(result, dict)





# @pytest.mark.skip
def test_get_latest_table_returns_correct_list_of_dicts(test_list):
    # Arrange
    expected_1 = 'design'
    expected_2 = list
    expected_2 = list
    # mock_table_data = { 'design': [{"design_id": 1, "xxx": "aaa"}, {"design_id": 2, "yyy": "bbb"}, {"design_id": 3, "zzz": "ccc"}] }
    expected_list_of_keys = ['design_id', 'xxx', 'design_id', 'yyy', 'design_id', 'zzz']
    expected_list_of_values = [1, 'aaa', 2, 'bbb', 3, 'ccc']
    result_list_of_keys = []
    result_list_of_values = []

    # Act
    response = get_latest_table(test_list[0], test_list[1], test_list[2])
    (key, value), = response.items() # value is hopefully a list of dicts
    result_1 = key          # hopefully string 'design'
    
    result_2 = type(value)  # hopefully a list 
    
    result_3 = True         # test that each member of list value is a dict
    for i in range(len(value)):
        if not isinstance(value[i], dict):
            result_3 = False
            break    

    # test that each member of list 
    # returned by get_latest_table()
    # has correct key-value pairs:            
    result_4 = True
    table_data = test_list[3]['design'] # expected list of dicts
    for i in range(len(value)):
        for res_key, res_value in value[i].items(): # eg design_id 2, yyy bbb
            result_list_of_keys.append(res_key)
            result_list_of_values.append(res_value)


    # Assert:
    assert result_1 == expected_1
    assert result_2 == expected_2
    assert result_list_of_keys == expected_list_of_keys
    assert result_list_of_values == expected_list_of_values

    


# @pytest.mark.skip    
def test_get_latest_table_raises_ClientError():
    # Mock an S3 client
    mock_s3 = Mock()

    # Ensure mock_s3 has a get_object 
    # method that raises a ClientError:
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist"}},
        "GetObject"
                                                )

    # Mock resp_dict:
    mock_resp_dict = {"Contents": [{"Key": "design/fake.json"}]}
    bucket_name = "ingestion-bucket"

    # Test whether get_latest_table() 
    # raises a ClientError:
    with pytest.raises(ClientError):
        get_latest_table(mock_resp_dict, mock_s3, bucket_name)




# @pytest.mark.skip    
def test_get_latest_table_logs_correctly(caplog, test_list):
    mock_resp_dict, mock_s3_client, mock_bucket_name, mock_table_data = test_list

    # Mock an S3 client (don't 
    # use the one in test_list)
    mock_s3 = Mock()

    # Ensure mock_s3 has a get_object 
    # method that raises a ClientError:
    mock_s3.get_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist"}},
        "GetObject"
                                                )

    caplog.set_level(logging.ERROR, logger="get_latest_table")

    with pytest.raises(ClientError):
        get_latest_table(mock_resp_dict, mock_s3, 'ingestion bucket')

    error_message = errors_lookup['err_5'] + 'design'
    assert any(error_message in msg for msg in caplog.messages)        


