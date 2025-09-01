import pytest 

from unittest.mock import Mock, patch


from src.second_lambda.second_lambda_utils.make_dim_or_fact_table import make_dim_or_fact_table






@pytest.fixture
def general_setup():
        mock_tbl_name_1  = 'staff'
        mock_tbl_name_2  = 'counterparty'
        mock_tbl_name_3  = 'location'
        mock_tbl_1       = 'dept or addr table' 
        mock_tbl_2       = 'dim or fact table' 
        mock_tabl_python = [{}, {}] 
        mock_s3_client   = 'mock_s3_client'
        mock_ing_bucket  = 'mock_ing_bucket'


        yield mock_tbl_name_1, mock_tbl_name_2, mock_tbl_name_3, mock_tbl_1, mock_tbl_2, mock_tabl_python, mock_s3_client, mock_ing_bucket





# @pytest.mark.skip
def test_correct_calls_to_internal_functions_when_table_name_is_staff(general_setup):
    # Arrange:
    (mock_tbl_name_1, 
     mock_tbl_name_2, 
     mock_tbl_name_3, 
     mock_tbl_1, 
     mock_tbl_2, 
     mock_tabl_python, 
     mock_s3_client, 
     mock_ing_bucket) = general_setup

    expected = ['staff_dim_table']
    # expected = ''

    # Act:
    with patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.make_staff_or_cp_dim_table') as mock_msocdt, \
        patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.func_lookup_table') as mock_flt: 
        mock_msocdt.return_value = ['staff_dim_table']
        result = make_dim_or_fact_table(mock_tbl_name_1, mock_tabl_python, mock_s3_client, mock_ing_bucket)
        mock_msocdt.assert_called_once_with(mock_tbl_name_1, mock_tabl_python, mock_flt, mock_ing_bucket, 'department', mock_s3_client)
        assert result == expected


# @pytest.mark.skip
def test_correct_calls_to_internal_functions_when_table_name_is_counterparty(general_setup):
    # Arrange:
    (mock_tbl_name_1, 
     mock_tbl_name_2, 
     mock_tbl_name_3, 
     mock_tbl_1, 
     mock_tbl_2, 
     mock_tabl_python, 
     mock_s3_client, 
     mock_ing_bucket) = general_setup

    expected = ['counterparty_dim_table']
    # expected = ''

    # Act:
    with patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.make_staff_or_cp_dim_table') as mock_msocdt, \
        patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.func_lookup_table') as mock_flt: 
        mock_msocdt.return_value = ['counterparty_dim_table']
        result = make_dim_or_fact_table(mock_tbl_name_2, mock_tabl_python, mock_s3_client, mock_ing_bucket)

        # Assert:
        mock_msocdt.assert_called_once_with(mock_tbl_name_2, mock_tabl_python, mock_flt, mock_ing_bucket, 'address', mock_s3_client)
        assert result == expected


# @pytest.mark.skip
def test_correct_calls_to_internal_functions_when_table_name_is_location(general_setup):
    # Arrange:
    (mock_tbl_name_1, 
     mock_tbl_name_2, 
     mock_tbl_name_3, 
     mock_tbl_1, 
     mock_tbl_2, 
     mock_tabl_python, 
     mock_s3_client, 
     mock_ing_bucket) = general_setup

    expected = ['location_dim_table']
    # expected = ''

    # Act:
    with patch('src.second_lambda.second_lambda_utils.make_dim_or_fact_table.func_lookup_table') as mock_flt: 
        mock_flt_returned_func = Mock()
        mock_flt_returned_func.return_value = ['location_dim_table']
        mock_flt.return_value = mock_flt_returned_func
        result = make_dim_or_fact_table(mock_tbl_name_3, mock_tabl_python, mock_s3_client, mock_ing_bucket)

        # Assert:
        mock_flt.assert_called_once_with(mock_tbl_name_3)
        assert result == expected

