def table_to_postgres_inserts_with_types(table, table_name, column_types=None):
    """
    Convert table to PostgreSQL INSERT queries with explicit type casting.
    
    Args:
        table: List of dictionaries
        table_name: Name of the PostgreSQL table
        column_types: Dict mapping column names to PostgreSQL types
                     e.g., {'salary': 'NUMERIC(10,2)', 'name': 'VARCHAR(100)'}
        
    Returns:
        list: List of SQL INSERT query strings with type casting
    """
    from datetime import datetime, date, time
    
    def format_value(value):
        """Format a Python value for PostgreSQL SQL."""
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
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
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"
    
    queries = []
    
    for row in table:
        columns = list(row.keys())
        values = [row[col] for col in columns]
        
        # Format column names
        column_list = ', '.join([f'"{col}"' for col in columns])
        
        # Format values with optional type casting
        formatted_values = []
        for col, val in zip(columns, values):
            formatted_val = format_value(val)
            
            # Add type cast if specified
            if column_types and col in column_types:
                formatted_val = f"{formatted_val}::{column_types[col]}"
            
            formatted_values.append(formatted_val)
        
        value_list = ', '.join(formatted_values)
        
        # Create INSERT query
        query = f'INSERT INTO "{table_name}" ({column_list}) VALUES ({value_list});'
        queries.append(query)
    
    return queries