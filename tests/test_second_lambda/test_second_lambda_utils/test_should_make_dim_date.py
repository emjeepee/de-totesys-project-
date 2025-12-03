import pytest


from datetime import datetime
from unittest.mock import Mock, patch, ANY
from src.second_lambda.second_lambda_utils.should_make_dim_date import should_make_dim_date



@pytest.fixture(scope="function")
def general_setup():
    mock_ifrop_1 = Mock()
    mock_ifrop_1.return_value = False
    mock_ifrop_2 = Mock()
    mock_ifrop_2.return_value = True
    mock_cddP = Mock()
    mock_uts3 = Mock()    
    mock_cddP.return_value = ['1', '2']
    
    yield mock_ifrop_1, mock_ifrop_2, mock_cddP, mock_uts3



# @pytest.mark.skip
def test_does_nothing_if_ifrop_returns_False(general_setup):
    # Arrange:
    (mock_ifrop_1, mock_ifrop_2, mock_cddP, mock_uts3) = general_setup

    # Act:
    # should_make_dim_date(ifrop,        cddP,      uts3,      start_date, timestamp_string, num_rows, proc_bucket, s3_client, )    
    should_make_dim_date(  mock_ifrop_1, mock_cddP, mock_uts3, ANY,        ANY,              ANY,      'pb',        's3_client')
    
    # Assert:
    # mock_cddP()
    # mock_uts3()
    # ifrop(proc_bucket, s3_client)
    mock_ifrop_1.assert_called_once_with('pb', 's3_client')
    mock_cddP.assert_not_called()
    mock_uts3.assert_not_called()
    
    



# @pytest.mark.skip
def test_passed_in_functions_called(general_setup):
    # Arrange:
    (mock_ifrop_1, mock_ifrop_2, mock_cddP, mock_uts3) = general_setup
    
    # Act:
    # should_make_dim_date(ifrop,   cddP,      uts3,      start_date,         timestamp_string, num_rows, proc_bucket,          s3_client, )
    should_make_dim_date(mock_ifrop_2, mock_cddP, mock_uts3, datetime(24, 1, 1), 'timestamp',     3,        '11-processed_bucket', 's3_client')
    
    # Assert:
    # ifrop(proc_bucket, s3_client)
    mock_ifrop_2.assert_called_once_with('11-processed_bucket', 's3_client')
    
    # cddP(start_date, timestamp_string, num_rows)
    mock_cddP.assert_called_once_with(datetime(24, 1, 1),   'timestamp',   3)

    # uts3(s3_client, proc_bucket, arr[1], arr[0])
    mock_uts3.assert_called_once_with('s3_client',  '11-processed_bucket', mock_cddP.return_value[1], mock_cddP.return_value[0])
    
