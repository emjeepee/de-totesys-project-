import pytest

from unittest.mock import Mock, patch, ANY


from src.first_lambda.first_lambda_utils.change_after_time_timestamp import change_after_time_timestamp




# @pytest.mark.skip
def test_calls_get_timestamp_and_replace_timestamp_correctly():
    # Arrange
    bucket_name = Mock()
    s3_client = Mock()  
    ts_key = Mock()
    default_ts = Mock()


    # act:
    with patch('src.first_lambda.first_lambda_utils.change_after_time_timestamp.get_timestamp') as mock_gts, \
        patch('src.first_lambda.first_lambda_utils.change_after_time_timestamp.replace_timestamp') as mock_rts:
        change_after_time_timestamp(bucket_name,
                                    s3_client,
                                    ts_key,
                                    default_ts                      
                                    )

    # assert
    mock_gts.assert_called_once_with(
                                    s3_client,
                                    bucket_name,
                                    ts_key,
                                    default_ts                      
                                     )

    mock_rts.assert_called_once_with(
                                    s3_client,
                                    bucket_name,
                                    ts_key,
                                    ANY
                                     )



