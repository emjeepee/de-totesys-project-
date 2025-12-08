import pytest


from src.first_lambda.first_lambda_utils.insert_updated_rows import insert_updated_rows



@pytest.fixture(scope="module")
def set_up_rows_and_table():

    mock_old_table = [
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



    yield mock_old_table, mock_updated_rows, expected_updated_table



def test_returns_a_list_of_dictionaries(set_up_rows_and_table):
    # arrange:
    mock_old_table, mock_updated_rows, expected_updated_table = set_up_rows_and_table
    expected = list
    expected_0 = dict


    response = insert_updated_rows(3, mock_old_table, mock_updated_rows, "design_id")
    result = type(response)
    result_0 = type(response[0])
    result_1 = type(response[1])
    result_2 = type(response[2]) 

    assert result == expected
    assert result_0 == expected_0
    assert result_1 == expected_0
    assert result_2 == expected_0
    


def test_returns_correct_list_of_dictionaries(set_up_rows_and_table):
    # arrange:
    mock_old_table, mock_updated_rows, expected_updated_table = set_up_rows_and_table
    expected = expected_updated_table

    result = insert_updated_rows(3, mock_old_table, mock_updated_rows, "design_id")

    assert result == expected
