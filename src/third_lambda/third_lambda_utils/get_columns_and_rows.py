


def get_columns_and_rows(result):
    """
    This function:
        extracts data from a 
        DuckDB query result 
        object and from it 
        creates a string that 
        contains all of the 
        column names of a table 
        and a list that 
        contains tuples, each 
        tuple containing the 
        values in a row of the 
        table.


    Args:
        result: a DuckDB query 
        result object.

    Returns: 
        a list like this:
        [column_str, rows],
        where column_str is a 
        string containing the 
        column names of a 
        table and rows is a 
        list of tuples where 
        each tuple contains 
        the values in a row of 
        the table.
        
    """


    # result.description contains 
    # info about each column 
    # (name, type, etc) and 
    # desc[0] is the column name.
    # Get the column names:
    columns = [desc[0] for desc in result.description]
    # ["xxx", "yyy", "zzz", "abc"]

    # make a string of all 
    # column names with the 
    # names in double
    # quotes:
    column_str = ', '.join([f'"{col}"' for col in columns])
    # '"xxx", "yyy", "zzz", "abc"'
    
    # get all of the rows
    # as a list of tuples:
    rows = result.fetchall()
        # [
        #   (1,  'xxx',   75.50,   None, True ),
        #   (2,  'yyy',   82.00,   None, False), 
        #   (3,  'zzz',   69.75,   None, True ),
        #    etc
        # ]

    return [column_str, rows]