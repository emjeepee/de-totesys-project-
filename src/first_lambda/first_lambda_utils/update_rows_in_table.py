import json

def update_rows_in_table( rows_list: list, table_list: str, file_location: str ):
    
    """
    This function:
        Replaces outdated rows in a passed-in table 
         (which is in the form of a Python list of
         dictionaries, each dictionary representing 
         a row of the table) with updated versions 
         held in a passed-in list of dictionaries
         (each dictionary representing an updated 
         row).
    Args:
        1) rows_list: a python list of dictionaries, each
         dictionary representing a row that contains
         updated data. The number of dictionaries can be 
         from 1 to the number of rows in a whole table.
        2) table_list: the table that has to have its 
         outdated rows replaced. This is a Python list
         of dictionaries that came from the ingestion 
         bucket, each dictionary representing a row. 
         The number of dictionaries matches the number 
         of rows in the corresponding table in the ToteSys 
         database.
        3) file_location: the name of the table. This is
         also the first part of the key under which 
         the S3 bucket stores the table, the second part 
         being a timestamp. The key looks like this,
         for example: 'design/2025-05-28_15-45-03.json' 

    Returns:
        a python list of dictionaries that represents
         an updated table, each dictionary representing 
         a row. The number of dictionaries equals the 
         number of rows in a whole table in the ToteSys 
         database.  
        
    """


    # file_location is, eg, 'design'.
    # update_row and table_row below are 
    # dictionaries that include, eg, the 
    # key 'design_id' (for the design 
    # table).
    # Find a row in the table and find an
    # updated row where both have the same
    # values for key 'design_id'. Then 
    # replace the table row with the updated 
    # row:
    id_col_name = file_location + "_id"
    updated_table = []
    lngth = len(table_list)

    for i in range(lngth):
        match = False
        for dictn in rows_list:
            if table_list[i][id_col_name] == dictn[id_col_name]: 
                match = True
                break
        if match:
            updated_table.append(dictn)
        else:
            updated_table.append(table_list[i])        

    return updated_table


    
