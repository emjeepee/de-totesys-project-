import pytest

from unittest.mock import patch
from src.second_lambda.second_lambda_utils.func_lookup_table import func_lookup_table




@pytest.fixture
def general_setup():
    tables = ["sales_order", "staff", "address", "design", "counterparty", "currency"]
    
    yield tables


# @pytest.mark.skip
def test_returns_correct_function(general_setup):
    # Arrange:
    (tables) = general_setup 
    with patch('src.second_lambda.second_lambda_utils.func_lookup_table.transform_to_star_schema_fact_table') as mock_ttssfc, \
        patch('src.second_lambda.second_lambda_utils.func_lookup_table.transform_to_dim_staff') as mock_ttds, \
        patch('src.second_lambda.second_lambda_utils.func_lookup_table.transform_to_dim_location') as mock_ttdl, \
        patch('src.second_lambda.second_lambda_utils.func_lookup_table.transform_to_dim_design') as mock_ttdd, \
        patch('src.second_lambda.second_lambda_utils.func_lookup_table.transform_to_dim_counterparty') as mock_ttdc, \
        patch('src.second_lambda.second_lambda_utils.func_lookup_table.transform_to_dim_currency') as mock_ttdcurr: \

        # Act:    
        result_0 = func_lookup_table(tables[0])    
        result_1 = func_lookup_table(tables[1])    
        result_2 = func_lookup_table(tables[2])    
        result_3 = func_lookup_table(tables[3])    
        result_4 = func_lookup_table(tables[4])    
        result_5 = func_lookup_table(tables[5])    

        # Assert:
        # assert result_0 is "fail"
        # assert result_1 is "fail"
        # assert result_2 is "fail"
        # assert result_3 is "fail"
        # assert result_4 is "fail"
        # assert result_5 is "fail"

        assert result_0 is mock_ttssfc
        assert result_1 is mock_ttds
        assert result_2 is mock_ttdl
        assert result_3 is mock_ttdd
        assert result_4 is mock_ttdc
        assert result_5 is mock_ttdcurr
