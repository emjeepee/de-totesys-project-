def table_to_postgres_inserts(table, table_name):
    """
    Convert a list of dictionaries to PostgreSQL INSERT query strings.
    Handles datetime, date, time, None, strings, numbers, and booleans.
    
    Args:
        table: List of dictionaries (output from parquet_buffer_to_table)
        table_name: Name of the PostgreSQL table to insert into
        
    Returns:
        list: List of SQL INSERT query strings
    """
    from datetime import datetime, date, time
    
    def format_value(value):
        """Format a Python value for PostgreSQL SQL."""
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            # Escape single quotes by doubling them
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, datetime):
            return f"'{value.isoformat()}'"
        elif isinstance(value, date):
            return f"'{value.isoformat()}'"
        elif isinstance(value, time):
            return f"'{value.isoformat()}'"
        elif isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # Fallback: convert to string
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
    
    queries = []
    
    for row in table:
        # Get column names and values
        columns = list(row.keys())
        values = [row[col] for col in columns]
        
        # Format column names (wrap in quotes to handle special chars)
        column_list = ', '.join([f'"{col}"' for col in columns])
        
        # Format values
        value_list = ', '.join([format_value(val) for val in values])
        
        # Create INSERT query
        query = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({value_list});'
        queries.append(query)
    
    return queries