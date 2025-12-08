

def update_rows_in_table( updated_rows: list, 
                         whole_table: list, 
                         table_name: str ):
    
    """
    This function:
        1) Replaces outdated rows 
         in a passed-in table 
         (which is in the form of 
         a list of dictionaries, 
         each dictionary 
         representing a row of the 
         table) with updated 
         versions held in a 
         passed-in list of 
         dictionaries (each 
         dictionary representing 
         an updated row).

    Args:
        1) updated_rows: a python 
         list of dictionaries
         that represents a table
         from database totesys. 
         Each dictionary 
         represents a row that 
         contains updated data. 
         The number of dictionaries 
         can be from 1 to the 
         number of rows in a 
         whole table. eg:
         [
         {<updated row>},
         {<updated row>},
         etc
         ]

        2) whole_table: the table 
         that has to have its 
         outdated rows replaced. 
         This is a Python list
         of dictionaries that 
         came from the ingestion 
         bucket, each dictionary 
         representing a row. eg:
         [
         {<row>},
         {<row>},
         etc
         ]
 

        3) table_name: the name 
         of the table. This is
         also the first part of 
         the key under which the 
         S3 bucket must store the 
         table, the second part 
         being a timestamp. 
         The key looks like this: 
         'design/2025-05-28_15-45-03.json' 

         
    Returns:
        a python list of 
         dictionaries that 
         represents an updated 
         table, each dictionary 
         representing a row. The 
         number of dictionaries 
         equals the number of 
         rows in a whole table 
         in database totesys 
         database.  
        
    """


    # update_row and 
    # table_row below are 
    # dictionaries that 
    # include, eg, the key 
    # 'design_id' (for the 
    # design table).
    # Find a row in the 
    # table and find an
    # updated row where both 
    # have the same values 
    # for key 'design_id'. 
    # Then replace the table 
    # row with the updated 
    # row:
    id_col_name = table_name + "_id" # 'design_id'
    updated_table = []
    lngth = len(whole_table)

    for i in range(lngth):
        match = False
        
        for dictn in updated_rows:
            if whole_table[i][id_col_name] == dictn[id_col_name]: 
                match = True
                break

        if match:
            # append the updated row:
            updated_table.append(dictn)
        else:
            # append the updated row:
            updated_table.append(whole_table[i])        


    return updated_table


    
