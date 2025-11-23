from datetime import date, datetime
from decimal import Decimal



def make_column_defs(data):
    """
    This function:

        is called by function convert_to_parquet()

    
    Args: 
        data: a list of dictionaries
        that represents a table. 
        Each dictionary represents a 
        row and its key-value pairs 
        represent columnname-cellvalue 
        pairs.

    Returns:
        the col defs
    
    """

    # Make a lookup table to
    # allow duckdb to 
    # determine the type of 
    # the data in cells:
    type_map = {
    int: "INTEGER",
    float: "REAL",
    bool: "BOOLEAN",
    str: "TEXT",
    date: "DATE",
    datetime: "TIMESTAMP",
    Decimal: "NUMERIC",
    None: "TEXT"           # Hopefully this will correctly 
                           # take care of None values in any 
                           # table 
               }
    
    # Get the first row from the 
    # table:
    first_row = data[0] # {"some_col_name": "some_value", etc}

    # make column definitions:
    col_defs = ', '.join(     # "some_col_name INT, some_col_name TEXT ..."
        f"{col} {type_map.get(type(val))}"
        for col, val in first_row.items()
                        )
    
    return col_defs