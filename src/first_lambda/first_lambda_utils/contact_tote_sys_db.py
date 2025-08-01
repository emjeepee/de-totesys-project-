from pg8000.native import ProgrammingError

def contact_tote_sys_db(conn_obj, opt: int, after_time: str, table_name: str):
    """
    This function:
        Makes an SQL query to the ToteSys
         database, the nature of which 
         depends on the value of opt.
    
    Args:
        1) conn_obj: an instance of the 
         pg8000.native Connection class.        
        2) opt: an int the value of which 
         determines which SQL query this 
         function makes to the ToteSys
         database.
        3) after_time: a time stamp that will 
          always be the time of the last run 
          of the first lambda function. 
        4) table_name: the name of the 
         table that this function wants
         to ask the ToteSys database 
         about.
        
    Returns:
        The result of the SQL query this
        function made to the ToteSys database.        
    
    """
    
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position" if opt == 2 else f"SELECT * FROM {table_name} WHERE last_updated > :after_time LIMIT 20;"

    try: 
        return conn_obj.run(query, after_time=after_time)
    
    except ProgrammingError as e:
        raise RuntimeError("Error occurred in attempt to read ToteSys database") from e

    