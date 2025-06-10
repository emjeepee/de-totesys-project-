import pytest
import pandas as pd
from unittest.mock import patch, Mock
from src.second_util_for_3rd_lambda import (
    put_table_data_in_warehouse,
    convert_dataframe_to_SQL_query_string,
)


# @pytest.mark.skip(reason="Skipping this test to perform only the previous tests")


def test_function_convert_dataframe_to_SQL_query_string_returns_correct_list_of_query_strings():
    # arrange:
    # make a pythonised pandas DataFrame
    # of the kind passed in to the
    # function under test as second arg:
    test_df = pd.DataFrame(
        [
            {"id": 1, "name": "Abdul", "team": 1},
            {"id": 2, "name": "Neill", "team": 1},
            {"id": 3, "name": "Mukund", "team": 1},
        ]
    )

    # the query string list that the
    # function under test is expected
    # to return:
    expected_sql_qrs_list = [
        "INSERT INTO table_here (id, name, team) VALUES ('1', 'Abdul', '1');",
        "INSERT INTO table_here (id, name, team) VALUES ('2', 'Neill', '1');",
        "INSERT INTO table_here (id, name, team) VALUES ('3', 'Mukund', '1');",
    ]

    # act:
    result_sql_qrs_list = convert_dataframe_to_SQL_query_string("table_here", test_df)
    # assert:
    assert result_sql_qrs_list == expected_sql_qrs_list


def test_convert_dataframe_to_SQL_query_string_calls_iterrows_once():
    # arrange:
    # make a mock to represent
    # the dataFrame:
    mock_df = Mock()
    # give the mock sataFrame columns:
    mock_df.columns = ["id", "name"]

    # make a mock for iterrows(),
    # which convert_dataframe_to_SQL_query_string()
    # calls once on the dataFrame.
    # Maake mock_iterrows return an iterable:
    mock_iterrows = Mock(return_value=[(0, {"id": 3, "name": "Abdul"})])
    mock_df.iterrows = mock_iterrows

    convert_dataframe_to_SQL_query_string("table_here", mock_df)

    # assert:
    mock_iterrows.assert_called_once()


def test_put_table_data_in_warehouse_executes_all_queries():
    # Arrange
    query_list = [
        "INSERT INTO table1 VALUES (1, 'a')",
        "UPDATE table2 SET col = 'b' WHERE id = 1",
    ]

    # mock the Connection object
    mock_conn = Mock()

    # make a patch for conn_to_db() and
    # set the patch's return value to
    # mock COnnection object mock_conn;
    # also patch close_db():
    with patch(
        "src.second_util_for_3rd_lambda.conn_to_db", return_value=mock_conn
    ) as mock_conn_to_db, patch(
        "src.second_util_for_3rd_lambda.close_db"
    ) as mock_close_db:

        # Act
        put_table_data_in_warehouse(query_list)

        # Assert
        mock_conn_to_db.assert_called_once_with("WAREHOUSE")
        assert mock_conn.run.call_count == len(query_list)
        mock_close_db.assert_called_once()
