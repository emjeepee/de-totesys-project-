import duckdb
from io import BytesIO
from datetime import datetime, date, time

def parquet_buffer_to_table(parquet_buffer):
    """
    Convert a Parquet file in a BytesIO buffer back to a list of dictionaries.
    Preserves datetime, date, and time types.
    
    Args:
        parquet_buffer: BytesIO buffer containing Parquet data
        
    Returns:
        list: List of dictionaries with original data types preserved
    """
    # Reset buffer position to beginning
    parquet_buffer.seek(0)
    
    # Create DuckDB connection
    conn = duckdb.connect(':memory:')
    
    # Read from the buffer
    result = conn.execute("SELECT * FROM parquet_scan(?)", [parquet_buffer])
    
    # Get column names
    columns = [desc[0] for desc in result.description]
    
    # Fetch all rows
    rows = result.fetchall()
    
    # Convert to list of dictionaries
    table = []
    for row in rows:
        row_dict = {}
        for col_name, value in zip(columns, row):
            row_dict[col_name] = value
        table.append(row_dict)
    
    conn.close()
    
    return table