import logging 
import pytest


from pg8000.native import DatabaseError
from unittest.mock import Mock, patch, ANY
from src.third_lambda.third_lambda_utils.make_SQL_queries_to_warehouse import make_SQL_queries_to_warehouse
from src.third_lambda.third_lambda_utils.errors_lookup import errors_lookup



def test_runs_all_queries():
    # arrange:
    queries = [
        "INSERT INTO table_a VALUES (1, 2, 3)",
        "INSERT INTO table_b VALUES (4, 5, 6)"
              ]

    mock_conn = Mock()

    # act:
    result = make_SQL_queries_to_warehouse(queries, mock_conn)

    # assert:
    assert result is None
    assert mock_conn.run.call_count == len(queries)




def test_queries_passed_correctly():
    # arrange:
    queries = [
        "INSERT INTO table_a VALUES (1, 2, 3)",
        "INSERT INTO table_b VALUES (4, 5, 6)"
              ]

    mock_conn = Mock()


    # act:
    make_SQL_queries_to_warehouse(queries, mock_conn)

    # assert:    
    mock_conn.run.assert_any_call("INSERT INTO table_a VALUES (1, 2, 3)")
    mock_conn.run.assert_any_call("INSERT INTO table_b VALUES (4, 5, 6)")    




def test_logs_and_reraises_database_error():
    # arrange:
    mock_conn = Mock()
    mock_conn.run.side_effect = DatabaseError("error")

    queries = ["SELECT 1"]

    with patch(
        "src.third_lambda.third_lambda_utils.make_SQL_queries_to_warehouse.logger"
              ) as mock_logger:

        # act: 
        with pytest.raises(DatabaseError):
            make_SQL_queries_to_warehouse(queries, mock_conn)

        # assert:
        mock_logger.exception.assert_called_once_with(ANY)