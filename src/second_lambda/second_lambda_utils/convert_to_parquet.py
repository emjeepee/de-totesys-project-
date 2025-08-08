import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO




def convert_to_parquet(data):
    """
    This function:
        1) creates a pandas dataframe from the 
            passed-in Python list of 
            dictionaries.
        2) creates a buffer in memory
        3) creates a pyarrow table from the
            Pandas dataframe.
        4) writes the pyarrow table to the 
            buffer as a Parquet file.
        5) Returns the buffer file.

    Args:
        data: A Python list of dictionaries
         that represents either the facts table 
         or a dimension table.

    Returns:         
        a Parquet version of either the facts
        table or a dimension table in a 
        buffer in memory.
    """

    df = pd.DataFrame(data)

    buffer = BytesIO()

    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)

    buffer.seek(0)

    return buffer
