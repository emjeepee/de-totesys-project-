# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from io import BytesIO




# def convert_to_parquet(data):
#     """
#     This function:
#         1) creates a pandas dataframe from the 
#             passed-in Python list of 
#             dictionaries.
#         2) creates a buffer in memory
#         3) creates a pyarrow table from the
#             Pandas dataframe.
#         4) writes the pyarrow table to the 
#             buffer as a Parquet file.
#         5) Returns the buffer file.

#     Args:
#         data: A Python list of dictionaries
#          that represents either the fact table 
#          or a dimension table. Takes this form:
#             [{<row data>}, {<row data>}, etc]
#             where {<row data>} is, eg,
#             {
#                 'design_id': 123, 
#                 'abcdef': 'xxx', 
#                 'design_name': 'yyy', 
#                 etc
#             }                                         

#     Returns:         
#         a Parquet version of either the fact
#         table or a dimension table in a 
#         buffer in memory.
#     """

#     df = pd.DataFrame(data)

#     buffer = BytesIO()

#     table = pa.Table.from_pandas(df)
#     pq.write_table(table, buffer)

#     buffer.seek(0)

#     return buffer




import duckdb

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
                'xyz': 'xxx', 
                'design_name': 'yyy', 
                    etc 
            }

    Returns:
        a buffer that contains a 
        Parquet-file version of the 
        passed-in fact table or  
        dimension table.  
    
    """


    con = duckdb.connect(database=':memory:')  # In-memory database
    
    # Get column names from the table dict:
    columns = list(data[0].keys())
    col_defs = ', '.join([f"{col} TEXT" for col in columns])

    # Create a temp table:
    con.execute(f"CREATE TABLE dim_or_fact_table ({col_defs});")

    # Insert each row:
    for row in data:
        placeholders = ', '.join(['?'] * len(columns))
        con.execute(f"INSERT INTO table VALUES ({placeholders});", tuple(str(v) for v in row.values()))

    buffer = BytesIO()

    con.execute("COPY dim_or_fact_table TO buffer (FORMAT PARQUET)")
    
    buffer.seek(0)

    return buffer.getvalue()









# ================================================================================
# OLD CODE (employed when this project used 
# Pandas and Pyarrow)
    # df = pl.DataFrame(data)

    # buffer = BytesIO()

    # df.write_parquet(buffer)

    # buffer.seek(0)

    # return buffer