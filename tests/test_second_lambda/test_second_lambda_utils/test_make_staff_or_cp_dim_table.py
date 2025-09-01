import pytest

from unittest.mock import Mock, patch
from src.second_lambda.second_lambda_utils.make_staff_or_cp_dim_table import make_staff_or_cp_dim_table




# test that this function:
# 1) returns a list of dictionaries
# 2) calls get_latest_table(s3_client, ingestion_bucket, aux_table_name)
#    once with correct args
# 3) calls function_lookup_table(table_name) once with correct args
# returns correct table

# make_staff_or_cp_dim_table(
#         table_name: str, 
#         table_python: list, 
#         function_lookup_table, 
#         ingestion_bucket: str, 
#         aux_table_name: str, 
#         s3_client):

@pytest.fixture
def general_setup():
        
        mock_table_name     = 'mock_table_name'
        mock_table          = [{}, {}]
        mock_bucket_name    = 'ing_bucket'
        mock_aux_table_name = 'mock_aux_table_name'
        mock_s3_client      = 'mock_s3_client' 

        yield mock_table_name, mock_table, mock_bucket_name, mock_aux_table_name, mock_s3_client


# @pytest.mark.skip
def test_returns_correct_list_of_dictionaries(general_setup):
    # Arrange:
    (mock_table_name, mock_table, mock_bucket_name, mock_aux_table_name, mock_s3_client) = general_setup
     
    with patch('src.second_lambda.second_lambda_utils.make_staff_or_cp_dim_table.get_latest_table') as mock_glt, \
        patch('src.second_lambda.second_lambda_utils.make_staff_or_cp_dim_table.func_lookup_table') as mock_flt:        
        mock_flt_returned_function = None
        mock_flt_returned_function = Mock()
        mock_flt.return_value = mock_flt_returned_function
        mock_flt_returned_function.return_value = [{}, {}, {}]
        result = make_staff_or_cp_dim_table(mock_table_name, mock_table, mock_flt, mock_bucket_name, mock_aux_table_name, mock_s3_client)
        
        # Assert:
        mock_glt.assert_called_once_with(mock_s3_client, mock_bucket_name, mock_aux_table_name)
        mock_flt.assert_called_once_with(mock_table_name)
        
        expected = [{}, {}, {}]
        assert result == expected





