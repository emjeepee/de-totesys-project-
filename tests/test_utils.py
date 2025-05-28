from src.utils import read_table
from unittest.mock import Mock
import pytest


class TestReadTable:
    def test_runs_query(self):
        # arrange
        #   create connection to dummy db with a table with a last_updated column
        #   create an after time (could just be the start of time)
        test_conn = Mock()
        test_conn.run.return_value = (
            "{result: {id: 2, last_updated: '2010-01-01', value: 'row 2'}}"
        )
        test_table_name = "test_table"
        test_time = "2005-01-01"
        # act
        result = read_table(test_table_name, test_conn, test_time)
        # assert
        test_conn.run.assert_called_once_with(
            f"""
        SELECT * FROM :table_name
        WHERE last_updated > :after_time
        """,
            table_name=test_table_name,
            after_time=test_time,
        )
        assert result == "{result: {id: 2, last_updated: '2010-01-01', value: 'row 2'}}"

    # def test_raises_value_error(self):
    #     test_conn = Mock()
    #     test_conn.side_effect = Exception
    #     with pytest.raises(ValueError):
    #         read_table("table name", test_conn, "time")
