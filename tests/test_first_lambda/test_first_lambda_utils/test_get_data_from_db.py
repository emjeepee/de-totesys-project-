from src.first_lambda.first_lambda_utils.get_data_from_db import get_data_from_db
from unittest.mock import Mock
import pytest




def test_get_data_from_db_returns_correct_list():
    # Arrange
    table_names = ['sales', 'design', 'transactions']

    mock_read_table = Mock()

    # Mock what read_table returns
    # when called three times (which 
    # it will be here because the 
    # table_names parameter is set 
    # to a list of three strings):

    read_table_1st_return =  {'sales': [{'sales_dict1_key': 'sales_dict1_value'}, {'sales_dict2_key': 'sales_dict2_value'}]}
    read_table_2nd_return =  {'design': [{'design_dict1_key': 'design_dict1_value'}, {'design_dict2_key': 'design_dict2_value'}]}
    read_table_3rd_return =  {'transactions': [{'trns_dict1_key': 'trns_dict1_value'}, {'trns_dict2_key': 'trns_dict2_value'}]}

    # Successive members of the list 
    # below are what read_table
    # returns on successive calls:
    mock_read_table = Mock(side_effect=[read_table_1st_return, read_table_2nd_return, read_table_3rd_return])
    
    expected = [read_table_1st_return, read_table_2nd_return, read_table_3rd_return]
    
    
    # Act
    result = get_data_from_db(table_names, 'timestamp', 'conn', mock_read_table)
    