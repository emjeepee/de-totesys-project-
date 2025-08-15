from src.first_lambda.first_lambda_utils.first_lambda_init import first_lambda_init
from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db


import pytest
import boto3
import pg8000.native
from botocore.client import BaseClient


# @pytest.mark.skip
def test_returns_a_dictionary():
    # Arrange
    expected = dict

    # Act
    returned = first_lambda_init()
    result = type(returned)

    # Assert
    assert result == expected







# @pytest.mark.skip
def test_dictionary_returned_has_correct_keys():
    # Arrange:

    # Act
    returned = first_lambda_init()
 
    # returned dict should be:        
        #  {  
        # 'tables': tables,  
        # 's3_client': boto3.client("s3"),
        # 'bucket_name': "11-ingestion-bucket",    
        # 'conn': conn_to_db("TOTE_SYS"),
        # 'close_db': close_db     
        #     }

    # Assert
    assert "tables" in returned
    assert "s3_client" in returned
    assert "bucket_name" in returned
    assert "conn" in returned
    assert "close_db" in returned







# @pytest.mark.skip
def test_dictionary_returned_has_correct_values_for_keys():
    # Arrange:
    tables = [
        "design",           
       "sales_order",      
        "counterparty",    
        "address",         
        "staff",            
        "department",      
        "currency"        
                ]

    # Act
    returned = first_lambda_init()

    # returned should be this dict:        
        #  {  
        # 'tables': tables,  
        # 's3_client': boto3.client("s3"),
        # 'bucket_name': "11-ingestion-bucket",    
        # 'conn': conn_to_db("TOTE_SYS"),
        # 'close_db': close_db     
        #     }

    # Assert
    
    assert returned['tables'] == tables
    assert isinstance(returned["s3_client"], BaseClient)
    assert returned["bucket_name"] == "11-ingestion-bucket"
    assert isinstance(returned["conn"], pg8000.native.Connection)
    assert returned["close_db"] is close_db

