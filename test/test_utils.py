from src.utils import read_table
import pg8000

class TestReadTable:
    def test_reads_table(self):
        # arrange
        #   create connection to dummy db with a table with a last_updated column
        #   create an after time (could just be the start of time)
        test_conn = ''
        test_table_name = 'test_table'
        test_time = '2005-01-01'
        # act
        result = read_table(test_table_name, test_conn, test_time)
        # assert
        assert isinstance(result, str)
        assert result == "{result: {id: 2, last_updated: '2010-01-01', value: 'row 2'}}"
        # assert result == <a dict-like string, one of the keys is last_updated>