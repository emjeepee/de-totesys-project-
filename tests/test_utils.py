from src.utils import read_table, convert_data
from unittest.mock import Mock
import pytest
import datetime


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
        SELECT * FROM {test_table_name}
        WHERE last_updated > :after_time
        """,
            after_time=test_time,
        )
        assert result == {
            "test_table": "{result: {id: 2, last_updated: '2010-01-01', value: 'row 2'}}"
        }

    # def test_raises_value_error(self):
    #     test_conn = Mock()
    #     test_conn.side_effect = Exception
    #     with pytest.raises(ValueError):
    #         read_table("table name", test_conn, "time")


class TestConvertToJson:
    def test_convert_valid_data_to_json(self):
        data = [{"id": 1, "name": "first_bag"}, {"id": 2, "name": "second_bag"}]
        json_str = convert_data(data)
        assert isinstance(json_str, str)

    def test_convert_contains_data_to_json(self):
        data = [{"id": 1, "name": "first_bag"}, {"id": 2, "name": "second_bag"}]
        json_str = convert_data(data)
        assert '"id": 1' in json_str

    def test_convert_data_invalid_data(self):
        class NonData:
            pass

        data = NonData

        with pytest.raises(ValueError) as error:
            convert_data(data)
        assert "ValueError" in str(error)

    def test_convert_data_datetime_to_iso_format(self):
        data = [
            {"id": 1, "name": "Alice", "last_updated": datetime.datetime(2025, 6, 1, 14, 30)},
            {"id": 2, "name": "Bob", "last_updated": datetime.datetime(2025, 6, 2, 10, 0)}
        ]
        
        expected= '[{"id": 1, "name": "Alice", "last_updated": "2025-06-01T14:30:00"}, {"id": 2, "name": "Bob", "last_updated": "2025-06-02T10:00:00"}]'
        
        result = convert_data(data)
        
        assert result == expected
        assert isinstance(result, str)