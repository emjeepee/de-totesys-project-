import duckdb
import tempfile

from io import BytesIO


def preprocess_buffer_1(buffer: BytesIO):
    """
    This function:
        creates:
            1) row_list: a list of 
            row values from a dimension 
            table or the fact table,
            eg [13, NULL, 'aubergine'] 
            
            2) cols: a list of strings,
            each string being a column 
            name, eg
            ['dim_design', 'xxx', 'yyy']

            3) cols_str: a string of 
            comma-separated column 
            names, eg
            'dim_design, xxx, yyy'.
    
    
    Args:
        buffer: a memory BytesIO buffer
        containing a Parquet file that
        represents a dimension table
        or the fact table. This comes
        from the processed bucket

    Returns:
        list [df, cols, cols_str]

    """
    # Create a temporary file 
    # with the ,parquet extension and write the buffer 
    # contents to it. Directory /tmp exists in AWS
    # and Linux and MacOS (but not Windows).
    # Delete the file when code leaves the with block:
    with tempfile.NamedTemporaryFile(suffix=".parquet", dir="/tmp", delete=True) as tmp_file:
        tmp_file.write(buffer.getvalue())
        tmp_file.flush() 

        # Create a DuckDB con 
        # object in RAM:
        con = duckdb.connect(database=":memory:")

        # Get DuckDB to read 
        # the parquet file
        result = con.execute(f"SELECT * FROM read_parquet('{tmp_file.name}')").fetchall()
        
        return [con, result]





# =====================================================================
# OLD CODE 2

    # buffer.seek(0)

    # # Read the Parquet data 
    # # into a DuckDB relation
    # # (a DuckDB table-like 
    # # object you can run SQL 
    # # queries on):
    # ddb_r = duckdb.read_parquet(buffer)


    # # convert ddb_r into an Apache 
    # # Arrow Table (fast, 
    # # column-based data structures
    # # held in memory):
    # arr_table = ddb_r.to_arrow()

    # # Convert arr_table
    # # into a list of dicts with 
    # # column names as keys. Each 
    # # dict is a row:
    # row_list = arr_table.to_pylist()  

    # # make a list of strings 
    # # that are column names, eg
    # # ['dim_design', 'xxx', 'yyy']
    # cols = list(row_list[0].keys())
    
    # # Make a comma-separated 
    # # string that contains all
    # # column names, eg 
    # # 'dim_design, xxx, yyy':
    # cols_str = ", ".join(cols) # 


    # return [row_list, cols, cols_str]











# ============================================================
# OLD CODE:


    # # Open a temp in-memory DuckDB 
    # # database (not saved to disk)
    # # that DuckDB will use to read 
    # # and query the Parquet data:
    # con = duckdb.connect(database=':memory:')

    # # Make a DuckDB dataframe from 
    # # the Parquet file in the 
    # # buffer. fetchall() gets all 
    # # the rows of data from the 
    # # Parquet file as a list of 
    # # tuples:
    # row_list = con.execute("SELECT * FROM read_parquet(?)", [buffer]).fetchall()

    # # Grab the column names of the 
    # # table:
    # cols = [desc[0] for desc in con.description] # ['dim_design', 'xxx', 'yyy']

    # # Join the column names with 
    # # commas:
    # cols_str = ", ".join(cols) # 'dim_design, xxx, yyy'

    # return [row_list, cols, cols_str]