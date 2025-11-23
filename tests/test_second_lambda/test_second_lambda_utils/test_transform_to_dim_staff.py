import pytest

from datetime import datetime, date, time
from unittest.mock import Mock, patch, ANY

from src.second_lambda.second_lambda_utils.transform_to_dim_staff import transform_to_dim_staff


@pytest.fixture
def general_setup():
    mock_st_table = [{'staff_id': 20, 'first_name': 'Aaaaa', 'last_name': 'AlastName', 'department_id': 8, 'email_address':'flavio.kulas@terrifictotes.com', 'created_at': datetime(2022, 11, 3, 14, 20, 51, 563000), 'last_updated':datetime(2023, 1, 3, 14, 20, 51, 563000)},
                     {'staff_id': 21, 'first_name': 'Bbbbbbb', 'last_name': 'BlastName', 'department_id': 9, 'email_address':'bbb.kulas@terrifictotes.com', 'created_at': datetime(2022, 10, 3, 14, 20, 51, 563000), 'last_updated':datetime(2022, 11, 3, 14, 20, 51, 563000)},
                     {'staff_id': 22, 'first_name': 'Cccccc', 'last_name': 'ClastName', 'department_id': 10, 'email_address':'ccc.kulas@terrifictotes.com', 'created_at': datetime(2022, 9, 3, 14, 20, 51, 563000), 'last_updated':datetime(2022, 11, 3, 14, 20, 51, 563000)}
                     ] 
    
    mock_de_table_1 = [
                     {'department_id': 8 , 'department_name': 'HR',         'location': 'Dublin',     'manager': 'Jesus Jones',     'created_at': datetime(2021, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2021, 11, 3, 14, 20, 49, 962000)},
                     {'department_id': 9 , 'department_name': 'Security',   'location': 'Dundee',     'manager': 'John Rotten',     'created_at': datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2022, 11, 3, 14, 20, 49, 962000)},
                     {'department_id': 10 , 'department_name': 'Cust Rels', 'location': 'Humberside', 'manager': 'Ronald McDonald', 'created_at': datetime(2023, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2023, 11, 3, 14, 20, 49, 962000)}
                     ]



    expected_1 = [
                     {'staff_id': 20, 'first_name': 'Aaaaa',   'last_name': 'AlastName', 'email_address':'flavio.kulas@terrifictotes.com', "department_name": 'HR',        "location": 'Dublin'},
                     {'staff_id': 21, 'first_name': 'Bbbbbbb', 'last_name': 'BlastName', 'email_address':'bbb.kulas@terrifictotes.com',    "department_name": 'Security',  "location": 'Dundee'},
                     {'staff_id': 22, 'first_name': 'Cccccc',  'last_name': 'ClastName', 'email_address':'ccc.kulas@terrifictotes.com',    "department_name": 'Cust Rels', "location": 'Humberside'}
                                ] 


    key_pairs = [
    ("department_name", "department_name"),
    ("location", "location"),
                ]

    
    yield mock_st_table, mock_de_table_1, expected_1, key_pairs





# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    mock_st_table, mock_de_table_1, expected_1, key_pairs = general_setup

    expected = list

    # Act
    # transform_to_dim_staff(staff_data, dept_data)
    response = transform_to_dim_staff(mock_st_table, mock_de_table_1)
    
    result = type(response)

    # Assert
    assert result == expected



# @pytest.mark.skip
def test_calls_function_make_dictionary_correctly(general_setup):
    # arrange:
    mock_st_table, mock_de_table_1, expected_1, key_pairs = general_setup
    
    with patch('src.second_lambda.second_lambda_utils.transform_to_dim_staff.make_dictionary') as mock_md:
        # Act:
        result = transform_to_dim_staff(mock_st_table, mock_de_table_1)

        # ensure test can fail:
        # assert mock_md.call_count == 2
        assert mock_md.call_count == 3
        # ensure test can fail:
        # mock_md.assert_any_call('ANY', 'key_pairs')
        mock_md.assert_any_call(ANY, key_pairs)





# @pytest.mark.skip
def test_returns_list_with_correct_key_value_pairs(general_setup):
    # Arrange
    mock_st_table, mock_de_table_1, expected_1, key_pairs = general_setup

    # Act
    # transform_to_dim_staff(staff_data, dept_data)
    result = transform_to_dim_staff(mock_st_table, mock_de_table_1)
   

    # Assert
    assert result == expected_1







