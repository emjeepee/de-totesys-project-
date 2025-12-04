import pytest
from unittest.mock import Mock, patch

from src.second_lambda.second_lambda_utils.put_pq_table_in_temp_file import put_pq_table_in_temp_file


@pytest.fixture
def setup():
    # def put_pq_table_in_temp_file(
    #           table_name: str, 
    #           col_defs: str, 
    #           values_list, 
    #           placeholders, 
    #           tmp_path: str
    #                               )

    test_table = [
        {"name": 'Mister Fantastic', "age": 44, "is_happy": True},
        {"name": 'Invisible Woman',  "age": 34, "is_happy": True},
        {"name": 'The Thing',        "age": 32, "is_happy": False}
                 ]

    vals_list = [
        ['Mister Fantastic', '44', 'True'],
        ['Invisible Woman', '34', 'True'],
        ['The Thing', '32', 'False']
                ]
    
    placeholders = '?, ?, ?'

    table_name = 'test_table'
    col_defs = 'name, age, is_happy'
    tmp_path = 'tmp_file_path'

    yield test_table, vals_list, placeholders, table_name, col_defs, tmp_path



def test_calls_functions_of_duckdb(setup):
    # Arrange:
    test_table, vals_list, placeholders, table_name, col_defs, tmp_path = setup
    # mock the connect method 
    # of DuckDB:
    mock_connect = Mock()
    # mock the return value 
    # of the connect method 
    # of DuckDB (ie conn):
    mock_conn = Mock()
    # mock the execute method
    # of conn:
    mock_conn.execute = Mock()

    with patch('src.second_lambda.second_lambda_utils.put_pq_table_in_temp_file.duckdb') as ddb:
        # arrange:
        ddb.connect = mock_connect
        mock_connect.return_value = mock_conn

        # act:
        # conn = duckdb.connect(':memory:')
        # conn.execute(f"CREATE TABLE {table_name} ({col_defs});")
        put_pq_table_in_temp_file(table_name, col_defs, vals_list, placeholders, tmp_path)

        # Assert:
        mock_connect.assert_called_once_with(':memory:')
        assert mock_conn.execute.call_count == 7 # 1 CREATE TABLE, 3 INSERT INTO, 3 COPY (SELECT ...

    # call_args_list gets info
    # on each call to the
    # function in question, 
    # including:
	# 1. the arguments
	# 2. the keyword args.
    # eg [
    # call('SELECT * FROM table;'), -> returns a list
    # call('another call')          -> returns a list
    # ]:
    calls = mock_conn.execute.call_args_list

    # Assert:
    # CREATE TABLE call
    the_create_call = calls[0] # call('CREATE TABLE etc')
    assert the_create_call.args[0].startswith("CREATE TABLE")

    # INSERT calls (next 3)
    the_insert_and_copy_calls = calls[1:7] # incl [1], not incl [4]
    # for call in the_insert_calls:
    #     # 
    #     assert call.args[0].startswith("INSERT INTO")
    for i in range (len(the_insert_and_copy_calls)):
        # for i is 0, 2, 4
        if i%2 == 0:
            assert the_insert_and_copy_calls[i].args[0].startswith("INSERT INTO")
        else:
        # for i is 1, 3, 5
            assert the_insert_and_copy_calls[i].args[0].startswith("COPY (SELECT * FROM ")

    assert len(the_insert_and_copy_calls) == 6

        

