import tempfile

from .write_parquet_to_buffer   import write_parquet_to_buffer
from .make_column_defs          import make_column_defs
from .make_insert_statements    import make_insert_statements
from .put_pq_table_in_temp_file import put_pq_table_in_temp_file




def convert_to_parquet(data: list, table_name: str):
    """
    This function:
        converts a table in the 
        form of a list of dictionaries 
        into a table in Parquet format
        in a BytesIO buffer.
    
    Args:
        data: a list of dictionaries
        representing a table, where 
        each dictionary represents a 
        row and whose key-value pairs 
        represent columnname-cellvalue 
        pairs.

        table_name: the name of the 
        table
        
    Returns:
        a BytesIO buffer that contains
        the table in Parquet format.
    """

    # make a string of column names:
    col_defs = make_column_defs(data)

    # makes parts of the insert 
    # statements that will insert 
    # row data (from the table 
    # passed in) into the duckdb
    # Parquet file that this 
    # function will make:
    ph_and_v_list = make_insert_statements(data)
    values_list  = ph_and_v_list[1]
    placeholders = ph_and_v_list[0]
    
    # Create a temporary file with a 
    # .parquet extension, put the file
    # in tmp_path. The location will 
    # be similar to '/tmp/xyz123.parquet'.
    # Don't delete the temporary file 
    # when the with block ends:
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
        tmp_path = tmp.name
    
    # make a duckdb table in Parquet 
    # format. the duckdb table column 
    # names and rows come from the 
    # passed-in list (arg data) that 
    # represents a dimension table or 
    # the fact table:
    put_pq_table_in_temp_file(table_name, col_defs, values_list, placeholders, tmp_path)

    pq_buffer = write_parquet_to_buffer(tmp_path)

    return pq_buffer
    



