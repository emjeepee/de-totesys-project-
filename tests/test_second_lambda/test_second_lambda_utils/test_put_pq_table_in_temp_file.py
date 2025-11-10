import duckdb
import pytest
from unittest.mock import Mock, patch

from src.second_lambda.second_lambda_utils.put_pq_table_in_temp_file import put_pq_table_in_temp_file


@pytest.fixture
def setup():
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
    # of ddb:
    mock_connect = Mock()
    # mock the return value 
    # of the connect method 
    # of ddb (ie conn):
    mock_conn = Mock()
    # mock the execute method
    # of conn:
    mock_conn.execute = Mock()

    with patch('src.second_lambda.second_lambda_utils.put_pq_table_in_temp_file.duckdb') as ddb:
        # Act:
        ddb.connect = mock_connect
        mock_connect.return_value = mock_conn

        # conn = duckdb.connect(':memory:')
        # conn.execute(f"CREATE TABLE {table_name} ({col_defs});")
        put_pq_table_in_temp_file(table_name, col_defs, vals_list, placeholders, tmp_path)

        # Assert:
        # Ensure test can fail:
        # mock_connect.assert_called_once_with('42')
        mock_connect.assert_called_once_with(':memory:')

        # Ensure test can fail:
        # assert mock_conn.execute.call_count == 8
        assert mock_conn.execute.call_count == 5

            # Get all calls made to execute()
    calls = mock_conn.execute.call_args_list

    # --- Assertions ---

    # Total calls
    assert len(calls) == 5  # 1 CREATE + 3 INSERT + 1 COPY

    # CREATE TABLE call
    create_call = calls[0]
    assert create_call.args[0].startswith("CREATE TABLE")

    # INSERT calls (next 3)
    insert_calls = calls[1:4]
    for call in insert_calls:
        assert call.args[0].startswith("INSERT INTO")
        assert len(call.args) == 2  # SQL + values
    assert len(insert_calls) == 3

    # COPY call (last one)
    copy_call = calls[-1]
    assert copy_call.args[0].startswith("COPY (SELECT * FROM")
    assert copy_call.args[1] == [tmp_path]
        

