import tempfile
import logging

from .write_parquet_to_buffer            import write_parquet_to_buffer
from .make_column_defs                   import make_column_defs
from .make_parts_of_insert_statements    import make_parts_of_insert_statements
from .put_pq_table_in_temp_file          import put_pq_table_in_temp_file





logger = logging.getLogger(__name__)

def convert_to_parquet(data: list, table_name: str):
    """
    This function:
        converts a table in the 
        form of a list of dictionaries 
        into a table in Parquet format
        in a BytesIO buffer.
        Function
        second_lambda_handler() calls
        this function.

    
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

    

    print(f"MY_INFO >>>>> In function convert_to_Parquet(). About to run function make_column_defs().")    
    # make a string of column names:
    col_defs = make_column_defs(data) # "some_col_name INT, some_col_name TEXT ..."


    # makes parts of the insert 
    # statements that will insert 
    # row data from the table
    # into the duckdb Parquet 
    # file that this function 
    # will make:
    print(f"MY_INFO >>>>> In function convert_to_Parquet(). About to run function make_parts_of_insert_statements().")    
    ph_and_v_list = make_parts_of_insert_statements(data)
    values_list  = ph_and_v_list[1]
    placeholders = ph_and_v_list[0]

    
    # Create a temporary file with  
    # extension .parquet, put the file
    # in tmp_path. The location will 
    # be similar to '/tmp/xyz123.parquet'
    # (where library tempfile generates 
    # random character string instead of 
    # xyz123. Don't delete the temporary 
    # file when the with block ends:
    # print(f"MY_INFO >>>>> In function convert_to_Parquet(). About to run with tempfile.NamedTemporaryFile().")    
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp:
        tmp_path = tmp.name


    # make a duckdb table in Parquet 
    # format and save it to the temp
    # location:
    # print(f"MY_INFO >>>>> In function convert_to_Parquet(). About to run put_pq_table_in_temp_file().")
    put_pq_table_in_temp_file(table_name, col_defs, values_list, placeholders, tmp_path)
 


    # Write the Parquet file in the 
    # temporary location to a BytesIO
    # buffer and permanently delete 
    # the temporary Parquet file 
    # at path tmp_path: 
    # print(f"MY_INFO >>>>> In function convert_to_Parquet(). About to run write_parquet_to_buffer().")
    pq_buffer = write_parquet_to_buffer(tmp_path)
                       

    return pq_buffer
    
