from .make_dictionary   import make_dictionary



def transform_to_dim_staff(staff_data, dept_data):
    """
    This function: 
        1) transforms the staff
        table that another function 
        previously read from the 
        ingestion bucket into a 
        staff dimension table.
        
        2) achieves 1) by looping 
        through the dictionaries in 
        the counterparty table. For 
        each dictionary, this function 
        finds the value of the key 
        'department_id' and finds 
        the dictionary in the address 
        table that has the same value 
        for its key 'department_id'.
        This function then creates new
        keys in the counterparty table,
        setting the values of those 
        keys to the values of specific 
        keys in the address table.
        For example: this function 
        will create key 
        "department_name" in the 
        dictionary in the staff table 
        and set its value to the value 
        of key "depratment_name" in the 
        dictionary from the department
        table.
        This function also ensures 
        that the staff dimension
        table it creates contains no 
        unneeded data from the 
        staff table.


    Args:
        staff_data: a list 
        of dicts. This is the 
        the staff table 
        from the ingestion bucket. 
        Each dict is a row.

        dept_data: a list of 
        dicts. This is the 
        department table from the 
        ingestion bucket. Each dict 
        is a row. 

    Returns:
        A list of dictionaries 
        that is the counterparty 
        dimension table, each 
        dict representing a row.         

    """

 
    # 1):
    key_pairs = [
    ("department_name", "department_name"),
    ("location", "location"),
                ]

    # for each row in the 
    # counterparty table find
    # the value of the key 
    # 'legal_address_id' and 
    # find the row in the address
    # table whose 'address_id' 
    # key has the same value:
    staff_dim_table = []
    for row_ST in staff_data:
        for row_DPT in dept_data:
            if row_ST['department_id'] \
                == row_DPT['department_id']:
                # For the row in the counterparty
                # table add new keys and values:
                new_st_row = make_dictionary(row_DPT, key_pairs)
                new_st_row['staff_id'] = row_ST['staff_id'] 
                new_st_row['first_name'] = row_ST['first_name'] 
                new_st_row['last_name'] = row_ST['last_name'] 
                new_st_row['email_address'] = row_ST['email_address'] 
                staff_dim_table.append(new_st_row) 

    

    return staff_dim_table



