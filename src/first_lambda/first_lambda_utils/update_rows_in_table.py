import json

def update_rows_in_table( rows_list: list, json_table_list, file_location: str ):
    
    """
    This function:
        Replaces outdated rows in a passed-in table 
         (which is in the form of a jsonified list) 
         with updated versions held in a passed-in 
         list.
    Args:
        1) rows_list: a python list of dictionaries, each
         dictionary representing a row that contains
         updated data. The number of dictionaries can be 
         from 1 to the number of rows in a whole table.
        2) json_table_list: the table that has to have  
         its outdated rows replaced. This is a jsonified 
         python list of dictionaries, each dictionary
         representing a row. The number of dictionaries
         matches the number of rows of the corresponding
         whole table in the ToteSys database.
        3) file_location: the name of the table. this is
         also the first part of the key under which 
         the S3 bucket stores the table. The second part 
         is a timestamp. The key looks like this,
         for example: 'design/<timestamp-here>' 

    Returns:
        a python list of dictionaries equal in number to
         the number of rows in a particular table in the
         ToteSys database. The list represents an
         updated table. Each dictionary in it represents 
         a row of the table, some of them now updated.
        
    """

    # convert json_table_list to a python list:
    table_list = json.load(json_table_list)

    # file_location is, eg, 'design'.
    # update_row and table_row are dictionaries
    # that include the key 'design_id' (for 
    # example).
    # Find a row in the table and find an
    # updated row where both have the same
    # values for key 'design_id'. Then 
    # replace the table row with the updated 
    # row:
    id_col_name = file_location + "_id"

    new_table_list = [ update_row     
      if update_row[id_col_name] == table_row[id_col_name] else table_row
      for update_row in rows_list    for table_row in table_list    
                     ]

    return new_table_list
