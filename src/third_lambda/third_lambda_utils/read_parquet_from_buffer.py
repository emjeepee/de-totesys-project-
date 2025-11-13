import duckdb
from io import BytesIO




def read_parquet_from_buffer(parquet_buffer, conn):
    """
    This function:

    
    Args:
        parquet_buffer: a
        dimension table or the 
        facts table table in 
        Parquet form in a BytesIO 
        buffer.

        conn: a duckdb connection 
        object.

    Returns:
        list [column_str, rows],
        where: 
            column_str: a string containing all
             column names, eg
                '"xx", "yyy", "zzz", "abcdef"'
            
            rows: a list of tupls, each 
             tuble being a row's values.

    """

    parquet_buffer.seek(0)
    
    # read the parquet data:
    result = conn.execute("SELECT * FROM parquet_scan(?)", [parquet_buffer])
        
    # result.description is 
    # info about each column 
    # (name, type, etc) and 
    # desc[0] is the column name.
    # Get the column names:
    columns = [desc[0] for desc in result.description]
    # ['xx', 'yyy', 'zzz', 'abcdef']

    # make a string of all column 
    # names with the names in double
    # quotes:
    column_str = ', '.join([f'"{col}"' for col in columns])
    # '"xx", "yyy", "zzz", "abcdef"'
    
    # get all of the rows
    # as a list of tuples:
    rows = result.fetchall()
        # [
        #   (1,  'xxx',   75.50,   datetime.date(2020, 1, 15) ),
        #   (2,  'yyy',   82.00,   datetime.date(2019, 6, 10) ),
        #   (3,  'zzz',   69.75,   datetime.date(2021, 3, 22) ),
        #    etc
        # ]

    return [column_str, rows]