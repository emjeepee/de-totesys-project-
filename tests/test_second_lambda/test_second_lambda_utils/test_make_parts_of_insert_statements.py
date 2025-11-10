from  src.second_lambda.second_lambda_utils.make_parts_of_insert_statements import make_parts_of_insert_statements

test_table = [
        {"name": 'Mister Fantastic', "age": 44, "is_happy": True},
        {"name": 'Invisible Woman',  "age": 34, "is_happy": True},
        {"name": 'The Thing',        "age": 32, "is_happy": False}
             ]

vals_list_of_lists = [
    ['Mister Fantastic', '44', 'True'],
    ['Invisible Woman', '34', 'True'],
    ['The Thing', '32', 'False']
                     ]


def test_returns_list():
    # Arrange:
    expected = list

    # Act
    response = make_parts_of_insert_statements(test_table)
    # Ensure test can fail:
    # result = str
    result = type(response)    

    # Assert:
    assert result == expected         


def test_returned_list_is_correct():
    # Arrange:
    expected_1 = '?, ?, ?'
    expected_2 = vals_list_of_lists

    # Act
    response = make_parts_of_insert_statements(test_table)
    # Ensure test can fail:
    # result_1 = "do you want some toast?"
    # result_2 = 'mr flibble'

    result_1 = response[0]    
    result_2 = response[1]    

    # Assert:
    assert result_1 == expected_1
    assert result_2 == expected_2         
