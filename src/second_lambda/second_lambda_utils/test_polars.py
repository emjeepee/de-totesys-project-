import polars as pl

from io import BytesIO
    
    
def convert_to_parquet(data):    
    """
    This function:
        1) creates a Parquet file from 
         a list that represents either 
         a fact table or a dimension 
         table (minus the headers) 
        2) puts the Parquet file into 
         a buffer 
        3) returns the buffer.

    Args:
        data: a table that represents 
        either a fact table or a 
        dimension table and has this
        form:
            [{<row data>}, {<row data>}, etc]
         where {<row data>} is, eg,
            {
                'design_id': 123, 
                'created_at': 'xxx', 
                'design_name': 'yyy', 
                    etc 
            }

    Returns:
        a buffer that contains a 
        Parquet-file version of the 
        passed-in fact table or  
        dimension table.  
    
    """

    df = pl.DataFrame(data)

    buffer = BytesIO()

    df.write_parquet(buffer)

    buffer.seek(0)

    return buffer