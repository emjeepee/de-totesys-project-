import pytest

from datetime import datetime

from src.second_lambda.second_lambda_utils.transform_to_dim_design import transform_to_dim_design




@pytest.fixture
def general_setup():

    # typical design table:
    des_tabl = [ 
        {'design_id': 654 , 'created_at': datetime(2025, 8, 12, 12, 11, 10, 73000),	'design_name': 'Fresh', 'file_location': '/Network', 'file_name': 'fresh-20240124-ap0b.json', 'last_updated': datetime(2025, 8, 12, 12, 11, 10, 73000)} , 
        {'design_id': 655 ,   'created_at': datetime(2025, 8, 12, 12, 11, 10, 73000),	'design_name': 'Fresh', 'file_location': '/Network', 'file_name': 'fresh-20240224-ap0b.json', 'last_updated': datetime(2025, 8, 12, 12, 11, 10, 73000)} , 
        {'design_id': 656 ,   'created_at': datetime(2025, 8, 12, 12, 11, 10, 73000),	'design_name': 'Fresh', 'file_location': '/Network', 'file_name': 'fresh-20240324-ap0b.json', 'last_updated': datetime(2025, 8, 12, 12, 11, 10, 73000)}  
 		      ]	

    
    expected = [ 
        {'design_id': 654 , 	'design_name': 'Fresh', 'file_location': '/Network', 'file_name': 'fresh-20240124-ap0b.json'} , 
        {'design_id': 655 , 	'design_name': 'Fresh', 'file_location': '/Network', 'file_name': 'fresh-20240224-ap0b.json'} ,
        {'design_id': 656 , 	'design_name': 'Fresh', 'file_location': '/Network', 'file_name': 'fresh-20240324-ap0b.json'} ,
 		       ]	

    yield des_tabl, expected




# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    (des_tabl, expected) = general_setup

    expected = list

    # Act
    # transform_to_dim_location(address_data)
    response = transform_to_dim_design(des_tabl)
    
    result = type(response)
    # result = None

    # Assert
    assert result == expected




# @pytest.mark.skip
def test_returns_correct_list(general_setup):
    # Arrange
    (des_tabl, expected) = general_setup


    # Act
    # transform_to_dim_location(address_data)
    result = transform_to_dim_design(des_tabl)
    # result = None
   

    # Assert
    assert result == expected


