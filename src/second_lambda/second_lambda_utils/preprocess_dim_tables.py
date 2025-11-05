


def preprocess_dim_tables(table: list, keys_to_cut: list):
    """
    This function:
        Removes certain keys from a list that 
        represents a whole table that came 
        from the ingestion bucket. The list 
        contains member dictionaries, each 
        dictionary representing a table row.
    
    Args:
        1) table: a whole table in the form of 
            a list of dictionaries, each
            dictionary representings a row of 
            the table. The table came from the 
            ingestion bucket. 
            The list looks like this:
            [{<row data>}, {<row data>}, etc]
            where {<row data>} is, eg,
                {
                    'design_id': 123, 
                    'design_name': 'yyy', 
                        etc 
                }
        2) keys_to_cut: a list of strings, each 
            representing a key. This function 
            will remove from each dictionary in 
            list table that key and its value.

    Returns:
        A new list that is a version of list 
         table but whose member dictionaries 
         no longer contain the keys whose 
         names are in list keys_to_cut. 
         The list looks like this:
         [{<row data>}, {<row data>}, etc]
         where {<row data>} is, eg,
         {
           'design_id': 123, 
           'design_name': 'yyy', 
            etc 
         }           
    
    """
    # make a deep copy of table:

    copy_list = [dict(item) for item in table]

    for dct in copy_list:
        for key in keys_to_cut:
            dct.pop(key, None)

    return copy_list
    
