import pytest

from unittest.mock import Mock, patch, ANY, call

from src.first_lambda.first_lambda_utils.get_data_from_db import get_data_from_db





@pytest.fixture
def test_vars():
    # mock a value for table_names:
    table_names = ['sales_order', 'design', 'transactions']

    # Mock what read_table returns
    # when called three times (which 
    # it will be here because the 
    # table_names parameter is set 
    # to a list of three strings):
    read_table_1st_return =  {'sales_order': [{'sales_dict1_key': 'sales_dict1_value'}, {'sales_dict2_key': 'sales_dict2_value'}]}
    read_table_2nd_return =  {'design': [{'design_dict1_key': 'design_dict1_value'}, {'design_dict2_key': 'design_dict2_value'}]}
    read_table_3rd_return =  {'transactions': [{'trns_dict1_key': 'trns_dict1_value'}, {'trns_dict2_key': 'trns_dict2_value'}]}

    rt_returns = [read_table_1st_return, read_table_2nd_return, read_table_3rd_return]

    list_1 = [
        table_names,
        rt_returns
             ]

    return list_1





# @pytest.mark.skip
def test_get_data_from_db_returns_a_list(test_vars):
    # get_data_from_db returns a 
    # python list of dictionaries. 
    # Each dictionary looks like 
    # this: 
    # {'<table_name_here>': [{<data from one row>}, {<data from one row>}, etc]}.

    # get_data_from_db(table_names: list, after_time: str, conn, read_table)
    # Arrange:
    expected = list
        # get the mock table_names:
    tn = test_vars[0]

    # get the mock return values of read_table():
    rt_1 = test_vars[1][0]
    rt_2 = test_vars[1][1]
    rt_3 = test_vars[1][2]

    mock_read_table = Mock()

    # Successive members of the list 
    # below are what read_table
    # returns on successive calls:
    mock_read_table = Mock(side_effect=[rt_1, rt_2, rt_3])

    # Act:
    response = get_data_from_db(tn, ANY, ANY, mock_read_table)
    result = type(response)

    # Assert
    assert result == expected







# @pytest.mark.skip
def test_get_data_from_db_returns_correct_list(test_vars):
    # Arrange
    # get the mock table_names:
    tn = test_vars[0]

    # get the mock return values of read_table():
    rt_1 = test_vars[1][0]
    rt_2 = test_vars[1][1]
    rt_3 = test_vars[1][2]

    mock_read_table = Mock()

    # Successive members of the list 
    # below are what read_table
    # returns on successive calls:
    mock_read_table = Mock(side_effect=[rt_1, rt_2, rt_3])
    expected = [rt_1, rt_2, rt_3]
    
    
    # Act
    result = get_data_from_db(tn, ANY, ANY, mock_read_table)

    # Assert 
    assert result == expected
    




# @pytest.mark.skip
def test_calls_utility_function_clean_data_correctly(test_vars):
    # arrange
    table_names = test_vars[0]
    after_time = "2025-01-01_00-00-00"
    

    tn = test_vars[0]

    # get the mock return values of read_table():
    rt_1 = test_vars[1][0]
    rt_2 = test_vars[1][1]
    rt_3 = test_vars[1][2]

    mock_read_table = Mock()

    # Successive members of the list 
    # below are what read_table
    # returns on successive calls:
    mock_read_table = Mock(side_effect=[rt_1, rt_2, rt_3])

    expected_calls = [
            call((rt_1,  tn[0])),
            call((rt_2), tn[1]),   
            call((rt_3), tn[2])   
                     ]



    # Act & Assert
    with patch('src.first_lambda.first_lambda_utils.get_data_from_db.clean_data') as mock_cd:
        get_data_from_db(table_names, ANY, ANY, mock_read_table)
        
        # table, table_dict
        mock_cd.call_count == 3
        # assert mock_cd.call_args_list == expected_calls



