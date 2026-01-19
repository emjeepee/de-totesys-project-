import pytest

from unittest.mock import Mock, patch

from src.first_lambda.first_lambda_utils.make_one_updated_table import make_one_updated_table




@pytest.fixture(scope="module")
def setup():

    mock_latest_table =  {
            'design': [
            {"design_id": 1, "name": "Terry", "team": 1, "project": "terraform"},
            {"design_id": 2, "name": "Barry", "team": 2, "project": "terraform"},
            {"design_id": 3, "name": "Gerry", "team": 3, "project": "terraform"},
                     ]
                      }                             

    mock_table_of_updates = {
            'design': [
            {"design_id": 1, "name": "Terry", "team": 42, "project": "terraform"},
            {"design_id": 2, "name": "Barry", "team": 13, "project": "terraform"}
                     ]
                            }
        
    mock_updated_table =  {
            'design': [
            {"design_id": 1, "name": "Terry", "team": 42, "project": "terraform"},
            {"design_id": 2, "name": "Barry", "team": 13, "project": "terraform"},
            {"design_id": 3, "name": "Gerry", "team": 3, "project": "terraform"},
                     ]
                        }      

    table_name = 'design'                               



    yield mock_latest_table, mock_table_of_updates, mock_updated_table, table_name







def test_make_one_updated_table_calls_internal_functions_corrrectly(setup):
    # arrange:
    mock_latest_table, mock_table_of_updates, mock_updated_table, table_name = setup
    expected = 'updated_table'

    # act:
    with patch('src.first_lambda.first_lambda_utils.make_one_updated_table.get_most_recent_table_data') as mock_gmrtd, \
        patch('src.first_lambda.first_lambda_utils.make_one_updated_table.update_rows_in_table') as mock_urit:
        mock_gmrtd.return_value = mock_latest_table
        mock_urit.return_value = 'updated_table'
        result = make_one_updated_table(mock_table_of_updates,
                                        's3_client',
                                        'bucket_name')

        # assert:
        mock_gmrtd.assert_called_once_with(table_name, 's3_client', 'bucket_name')
        mock_urit.assert_called_once_with(mock_table_of_updates[table_name],
                                          mock_latest_table,
                                          table_name)
        assert result == expected



