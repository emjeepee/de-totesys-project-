import os
from io import BytesIO
from unittest.mock import Mock, patch, mock_open


from src.second_lambda.second_lambda_utils.write_parquet_to_buffer import write_parquet_to_buffer






def test_write_parquet_to_buffer():
    mock_data = b"fake parquet data"
    mock_path = "/tmp/fake_file.parquet"
    mo = mock_open(read_data=mock_data)

    with patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.open", mo), \
        patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.os.path.exists", return_value=True), \
        patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.os.remove", return_value=''):
        # Act:            
        result = write_parquet_to_buffer(mock_path)

        # test the calling of open():
        mo.assert_called_once()
        handle = mo()
        handle.read.assert_called_once()


        
def test_calls_os_library_methods_correctly():
    """
    Also tests that return value of 
    write_parquet_to_buffer() is 
    correct.
    """
    # Arrange:
    mock_data = b"fake parquet data"
    mock_path = "/tmp/fake_file.parquet"
    mo = mock_open(read_data=mock_data)


    with patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.os.path.exists", return_value=True) as mock_exists, \
        patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.open", mo), \
        patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.os.remove") as mock_remove:
        
        handle = mo()
        

        # Act:        
        result = write_parquet_to_buffer(mock_path)

        # Assert:
        mock_exists.assert_called_once_with(mock_path)
        mock_remove.assert_called_once_with(mock_path)

        assert isinstance(result, BytesIO)
        assert result.getvalue() == mock_data
        assert result.tell() == 0  # ensure seek(0) was called



def test_returns_none_if_file_does_not_exist():
    """
    Also tests that write_parquet_to_buffer() 
    calls certain methods and not others.
    """
    # Arrange:
    mock_path = "/tmp/missing_file.parquet"

    with patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.os.path.exists", return_value=False) as mock_exists, \
         patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.open") as mock_open, \
         patch("src.second_lambda.second_lambda_utils.write_parquet_to_buffer.os.remove") as mock_remove:
        
        # Act:
        result = write_parquet_to_buffer(mock_path)

        # Assert
        mock_exists.assert_called_once_with(mock_path)
        mock_open.assert_not_called()
        mock_remove.assert_not_called()
        assert result is None