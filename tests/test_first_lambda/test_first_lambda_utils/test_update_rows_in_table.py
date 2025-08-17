import pytest

from src.first_lambda.first_lambda_utils.update_rows_in_table import update_rows_in_table
   



@pytest.fixture(scope="module")
def set_up_rows_and_table():

    mock_outdated_table = [
            {"design_id": 1, "name": "abdul", "team": 1, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 2, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"},
                             ]

    mock_updated_rows = [
            {"design_id": 1, "name": "abdul", "team": 42, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 13, "project": "terraform"}
                            ]
        
    expected_updated_table = [
            {"design_id": 1, "name": "abdul", "team": 42, "project": "terraform"},
            {"design_id": 2, "name": "Mukund", "team": 13, "project": "terraform"},
            {"design_id": 3, "name": "Neil", "team": 3, "project": "terraform"},
                             ]



    yield mock_outdated_table, mock_updated_rows, expected_updated_table



def test_update_rows_in_table_returns_a_list(set_up_rows_and_table):
    # Arrange
    ( mock_outdated_table, mock_updated_rows, expected_updated_table) = set_up_rows_and_table
    expected = list
    
    # Act:
    # update_rows_in_table(rows_list: list, table_list, file_location: str) 
    response = update_rows_in_table(mock_updated_rows, mock_outdated_table, 'design')
    result = type(response)
    
    # Assert:
    assert result == expected



def test_update_rows_in_table_returns_a_list_with_correct_number_of_members(set_up_rows_and_table):
    # Arrange
    ( mock_outdated_table, mock_updated_rows, expected_updated_table) = set_up_rows_and_table
    expected = len(mock_outdated_table)
    
    # Act:
    # update_rows_in_table(rows_list: list, table_list, file_location: str) 
    response = update_rows_in_table(mock_updated_rows, mock_outdated_table, 'design')
    result = len(response)
    
    # Assert:
    assert result == expected



def test_update_rows_in_table_returns_a_list_containing_only_dictionaries(set_up_rows_and_table):
    # Arrange
    ( mock_outdated_table, mock_updated_rows, expected_updated_table) = set_up_rows_and_table
    dict_count = 0
    expected = len(mock_outdated_table)

    
    # Act:
    # update_rows_in_table(rows_list: list, table_list, file_location: str) 
    response = update_rows_in_table(mock_updated_rows, mock_outdated_table, 'design')
    for item in response:
        if type(item) == dict:
            dict_count += 1

    result = dict_count

    
    # Assert:
    assert result == expected



# @pytest.mark.skip
def test_update_rows_in_table_returns_correct_list(set_up_rows_and_table):
    ( 
        mock_outdated_table, mock_updated_rows, expected_updated_table
    ) = set_up_rows_and_table


    # Arrange:
    

    # Act:
    result_table = update_rows_in_table(mock_updated_rows, mock_outdated_table, "design")

    # assert
    assert result_table == expected_updated_table
