

def make_parts_of_insert_statements(data):
    """
    This function:
        1. 
    
    Args:
        data: a list of dictionaries
        that represents a dimension 
        table or the fact table.

    Returns:
           [placeholders, values_list],
           placeholders: a string of 
           comma-separated ?s equal in 
           number to the number of 
           columns in the table.  
           values_list: a list of 
           row-value lists, where 
           each row value is a string
    """
    # Get the first row from the 
    # table and make a list of 
    # column names:
    columns = list(data[0].keys())

    # make string of ?s, the number 
    # of ?s equalling the number of 
    # members of the list columns:
    placeholders = ', '.join('?' for _ in columns)

    # Make a list of lists where 
    # the number of member lists 
    # will equal the number of 
    # rows in the table. Each
    # member list contains string 
    # versions of the values of
    # a row. 
    values_list = []
    for row in data:
        values = [str(row[col]) for col in columns]  
        values_list.append(values)

    return [placeholders, values_list]

