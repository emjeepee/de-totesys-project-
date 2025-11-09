from datetime import date, datetime
from decimal import Decimal



def make_column_defs(data):
    """
    This function:

    
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
    type_mapping = {
    int: "INTEGER",
    float: "REAL",
    bool: "BOOLEAN",
    str: "TEXT",
    date: "DATE",
    datetime: "TIMESTAMP",
    Decimal: "NUMERIC",
                   }
    
    # Get the first row from the 
    # table so that code can 
    # later determine the column
    # names:
    first_row = data[0] 

    # make column definitions:
    col_defs = ', '.join(
        f"{col} {type_mapping.get(type(val))}"
        for col, val in first_row.items()
                        )
    
    return col_defs