import pytest

from datetime import datetime, date, time


from src.second_lambda.second_lambda_utils.transform_to_dim_staff import transform_to_dim_staff


@pytest.fixture
def general_setup():
    # "staff" table cols and typical values
# [['staff_id'], ['first_name'], ['last_name'], ['department_id'], ['email_address'], ['created_at'], ['last_updated']]
# [[20, 'Flavio', 'Kulas', 3, 'flavio.kulas@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]]
    mock_st_table = [{'staff_id': 20, 'first_name': 'Aaaaa', 'last_name': 'AlastName', 'department_id': 8, 'email_address':'flavio.kulas@terrifictotes.com', 'created_at': datetime(2022, 11, 3, 14, 20, 51, 563000), 'last_updated':datetime(2023, 1, 3, 14, 20, 51, 563000)},
                     {'staff_id': 21, 'first_name': 'Bbbbbbb', 'last_name': 'BlastName', 'department_id': 9, 'email_address':'bbb.kulas@terrifictotes.com', 'created_at': datetime(2022, 10, 3, 14, 20, 51, 563000), 'last_updated':datetime(2022, 11, 3, 14, 20, 51, 563000)},
                     {'staff_id': 22, 'first_name': 'Cccccc', 'last_name': 'ClastName', 'department_id': 10, 'email_address':'ccc.kulas@terrifictotes.com', 'created_at': datetime(2022, 9, 3, 14, 20, 51, 563000), 'last_updated':datetime(2022, 11, 3, 14, 20, 51, 563000)}
                     ] 
    
    
# "department" table cols and typical values
# [['department_id'], ['department_name'], ['location'], ['manager'], ['created_at'], ['last_updated']]
# [[8, 'HR', 'Leeds', 'James Link', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]
    mock_de_table_1 = [
                     {'department_id': 8 , 'department_name': 'HR',         'location': 'Dublin',     'manager': 'Jesus Jones',     'created_at': datetime(2021, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2021, 11, 3, 14, 20, 49, 962000)},
                     {'department_id': 9 , 'department_name': 'Security',   'location': 'Dundee',     'manager': 'John Rotten',     'created_at': datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2022, 11, 3, 14, 20, 49, 962000)},
                     {'department_id': 10 , 'department_name': 'Cust Rels', 'location': 'Humberside', 'manager': 'Ronald McDonald', 'created_at': datetime(2023, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime(2023, 11, 3, 14, 20, 49, 962000)}
                     ]



    expected_1 = [
                     {'staff_id': 20, 'first_name': 'Aaaaa',   'last_name': 'AlastName', 'department_id': 8,  'email_address':'flavio.kulas@terrifictotes.com', "department_name": 'HR',        "location": 'Dublin'},
                     {'staff_id': 21, 'first_name': 'Bbbbbbb', 'last_name': 'BlastName', 'department_id': 9,  'email_address':'bbb.kulas@terrifictotes.com',    "department_name": 'Security',  "location": 'Dundee'},
                     {'staff_id': 22, 'first_name': 'Cccccc',  'last_name': 'ClastName', 'department_id': 10, 'email_address':'ccc.kulas@terrifictotes.com',    "department_name": 'Cust Rels', "location": 'Humberside'}
                                ] 



    dept_lookup_1 = { '8': {
            "department_name": 'HR',
            "location": 'Dublin',
                         }, 
                    '9': {
            "department_name": 'Security',
            "location": 'Dundee',
                         },                                     
                    '10': {
            "department_name": 'Cust Rels',
            "location": 'Humberside',
                          },                                     
                   }

    
    yield mock_st_table, mock_de_table_1, expected_1, dept_lookup_1





# @pytest.mark.skip
def test_returns_list(general_setup):
    # Arrange
    (mock_st_table, mock_de_table_1, expected_1, dept_lookup_1) = general_setup

    expected = list

    # Act
    # transform_to_dim_staff(staff_data, dept_data)
    response = transform_to_dim_staff(mock_st_table, mock_de_table_1)
    
    result = type(response)

    # result_1 = None
    # result_2 = None
   

    # Assert
    assert result == expected







# @pytest.mark.skip
def test_returns_list_with_correct_key_value_pairs(general_setup):
    # Arrange
    (mock_st_table, mock_de_table_1, expected_1, dept_lookup_1) = general_setup


    # Act
    # transform_to_dim_staff(staff_data, dept_data)
    result = transform_to_dim_staff(mock_st_table, mock_de_table_1)
    # result_1 = None
    # result_2 = None
    

    # Assert
    assert result == expected_1










# [
# {'staff_id': 20, 'first_name': 'Aaaaa', 'last_name': 'AlastName', 'department_id': 8, 'email_address': 'flavio.kulas@terrifictotes.com'}, 
# {'staff_id': 21, 'first_name': 'Bbbbbbb', 'last_name': 'BlastName', 'department_id': 9, 'email_address': 'bbb.kulas@terrifictotes.com'}, 
# {'staff_id': 22, 'first_name': 'Cccccc', 'last_name': 'ClastName', 'department_id': 10, 'email_address': 'ccc.kulas@terrifictotes.com'}
# ]