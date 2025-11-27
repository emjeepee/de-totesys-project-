from datetime import date, datetime, time
from decimal import Decimal



def make_column_defs(data):
    """
    This function:
        1. 
        
        2. is called by function 
        convert_to_parquet()

    
    Args: 
        data: a list of 
        dictionaries that 
        represents a dimension 
        table or the fact table. 
        Each dictionary 
        represents a row and its 
        key-value pairs 
        represent 
        columnname-cellvalue 
        pairs.

    Returns:
        a string that looks like 
        this:
        'col1_name, col2_name, ...' 
        
    
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
    time: "TIME",
    Decimal: "NUMERIC",
    None: "TEXT"           
               }
    
    # Get the first row from the 
    # table:
    first_row = data[0] # {"col_1_name": 13, "col_2_name": "val_2", etc}

    # make column definitions:
    col_defs = ', '.join(     # "col_1_name INT, "col_2_name TEXT, etc"
        f"{col} {type_map.get(type(val))}"
        for col, val in first_row.items()
                        )
    
    return col_defs