import pytest

from unittest.mock import Mock, patch


from src.second_lambda.second_lambda_utils.convert_to_parquet               import convert_to_parquet
from src.second_lambda.second_lambda_utils.make_column_defs                 import make_column_defs
from src.second_lambda.second_lambda_utils.make_parts_of_insert_statements  import make_parts_of_insert_statements
from src.second_lambda.second_lambda_utils.put_pq_table_in_temp_file        import put_pq_table_in_temp_file
from src.second_lambda.second_lambda_utils.write_parquet_to_buffer          import write_parquet_to_buffer



@pytest.fixture(scope="function")
def setup():
    # Define a simple class
    class Mock_tmp_class:
        def __init__(self):
            self.name = "Bathsheba"

    # Create an instance
    mock_tmp_obj = Mock_tmp_class()

    yield mock_tmp_obj



def test_calls_all_functions_correctly(setup):
    """
    This function also 
    tests that 
    convert_to_parquet() 
    returns the buffer 
    created by the last 
    function that 
    convert_to_parquet()
    calls. 
    """
    
    # Arrange:
    expected = 'buffer'

    mock_tmp_obj = setup

    
    mock_table = [{}, {}, {} ] # mock the 1st arg of convert_to_parquet()
    mock_table_name = 'mock_table_name'
    # mock_mcd_return = Mock()
    mock_vl = 'mock_vl'
    mock_ph = 'mock_ph'
    mock_ph_v_list = [mock_ph, mock_vl]
    mock_tmp = Mock()
    mock_path = 'mock_path'
    mock_tmp.name = mock_path
    # Following two lines do not work (hence the two after them)!!!
    # mock_tmp.__enter__.return_value = mock_tmp
    # mock_tmp.__exit__.return_value = None
    mock_tmp.__enter__ = Mock(return_value=mock_tmp)
    mock_tmp.__exit__ = Mock(return_value=None)



    with patch('src.second_lambda.second_lambda_utils.convert_to_parquet.make_column_defs') as mock_mcd, \
         patch('src.second_lambda.second_lambda_utils.convert_to_parquet.make_parts_of_insert_statements') as mock_mpois, \
         patch('src.second_lambda.second_lambda_utils.convert_to_parquet.put_pq_table_in_temp_file') as mock_pptitf, \
         patch('src.second_lambda.second_lambda_utils.convert_to_parquet.write_parquet_to_buffer') as mock_wtpb, \
         patch("src.second_lambda.second_lambda_utils.convert_to_parquet.tempfile.NamedTemporaryFile", return_value=mock_tmp) as mock_ntf:
        mock_mcd.return_value = 'mock_mcd_return'
        mock_mpois.return_value = mock_ph_v_list
        mock_wtpb.return_value = 'buffer'
        

        # Act:
        result = convert_to_parquet(mock_table, mock_table_name)

        # Assert:
        mock_mcd.assert_called_once_with(mock_table)                         
        mock_mpois.assert_called_once_with(mock_table)
        mock_mpois.assert_called_once_with(mock_table)
        mock_ntf.assert_called_once_with(delete=False, suffix='.parquet')
        mock_pptitf.assert_called_once_with(mock_table_name, 'mock_mcd_return', mock_vl, mock_ph, mock_path)
        mock_wtpb.assert_called_once_with(mock_path)
        assert result == expected






