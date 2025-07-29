from unittest.mock import Mock, patch, ANY
from src.first_lambda.first_lambda_utils.convert_data import convert_data

import datetime
from decimal import Decimal 

import json

# Arrange:    
dt = datetime.datetime(2025, 7, 29, 15, 30, 13)
dec = Decimal('3.14159')
input_dict = {"name": "mukund", "last_login": dt, "fave_value": dec}

# Act
json_dict = convert_data(input_dict)



def test_convert_data_returns_string():

    # Arrange:
        # see above

    # Act:
        # see above    

    # Assert
    assert isinstance(json_dict, str)





def test_dictionary_passed_in_to_convert_data_reconverts_to_correct_dictionary():
    # Test that when a dictionary is passed in to convert_data() 
    # and the resulting json string is reconverted back using
    # json.loads() that the resulting dictionary contains the 
    # correct values for the keys:

    # Arrange:
        # see above

    # Act:
        # see above    
    reconverted_dict = json.loads(json_dict)
    
    # Assert
    assert reconverted_dict["name"] == "mukund"
    assert reconverted_dict["last_login"] == dt.isoformat()
    assert reconverted_dict["fave_value"] == '3.14159'

    