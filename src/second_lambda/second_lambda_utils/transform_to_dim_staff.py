from .preprocess_dim_tables import preprocess_dim_tables



def transform_to_dim_staff(staff_data, dept_data):
    """
    This function:
        transforms the staff table data that came from the 
         ingestion bucket (that code earlier converted 
         into a python list) into a staff dimension table.
         In doing so this function creates a lookup table
         for department data. This is in the form of a
         dictionary.

    Args:
        staff_data: a list of dictionaries that represents 
         the staff table that came from the ingestion 
         bucket (and is now unjsonified). Each dictionary 
         in the list represents a row. Each key-value pair 
         in the dictionary represents a 
         columnName-cellValue pair.
        dept_data: a list of dictionaries that represents 
         the department table and that came from the ingestion 
         bucket (and is now unjsonified). Each dictionary 
         in the list represents a row. Each key-value pair 
         in the dictionary represents a 
         columnName-cellValue pair.

    Returns:
        A list of dictionaries that is the staff dimension 
         table.    
    """
    # The department table has these column names and types:
    # 'department_id',  'department_name',  'location',  'manager',  'created_at',          'last_updated' 
    #  int                  str              str            str       datetime.datetime     datetime.datetime
    
    # Make a lookup dictionary that the department 
    # table will employ:
    # {
    #  '15': {"department_name": 'aaaa aaa', "location": 'bbbbb'},
    #  '16': {"department_name": 'cccc ccc', "location": 'ddddd'},
    #           etc
    # }
    department_lookup = {  # if dept_data contains 2+ dicts in
                           # which key "department_id" has the 
                           # same value each will produce the 
                           # same key-value pair in 
                           # department_lookup, the current 
                           # one replacing the previous one.
        str(dept["department_id"]): {
            "department_name": dept.get("department_name"),
            "location": dept.get("location"),
                                    }
        for dept in dept_data
                        }
    

    print(f'In tx_to_dim_staff() and dict department_lookup is >>>>>>>>>> {department_lookup}')
    print('\n')
    print('\n')
    print(f'In tx_to_dim_staff() department_lookup.get(8) is >>>>>>>>>> {department_lookup.get('8')}' )
    print('\n')
    print('\n')
    



    # create a preprocessed staff dimension table:
    pp_staff_dim_table = preprocess_dim_tables(staff_data, ['created_at', 'last_updated'])

    print(f'In tx_to_dim_staff(), before for loop and pp_staff_dim_table is >>>>>>>>>> {pp_staff_dim_table}')            
    print('\n')
    print('\n')

    # add keys to the preprocessed staff dimension table
    # to make the final staff dimension table:
    for i in range(len(pp_staff_dim_table)):
        # try:
            # department_lookup is a dictionary that looks like this:
                # {
                #  '1': {"department_name": 'aaaa aaa', "location": 'bbbbb'},
                #  '2': {"department_name": 'cccc ccc', "location": 'ddddd'},
                #  etc
                # }
            print(f'In for loop in tx_to_dim_staff()')            
            print(f'len(pp_staff_dim_table) is >>>>>>> {len(pp_staff_dim_table)}')
            print(f'i is >>>>>>>>>> {i}')
            print(f'staff_data[i] is >>>>>>>>>> {staff_data[i]}')
            print(f'staff_data[i].get("department_id") is >>>>>>>>>> {staff_data[i].get("department_id")}')
            print(f'type(staff_data[i].get("department_id")) is >>>>>>>>>> {type(staff_data[i].get("department_id"))}')
            print(f'str(staff_data[i].get("department_id")) is >>>>>>>>>> {str(staff_data[i].get("department_id"))}')
            print(f'type(str(staff_data[i].get("department_id"))) is >>>>>>>>>> {type(str(staff_data[i].get("department_id")))}')

            dept = department_lookup.get(
                        str(staff_data[i].get("department_id")) # '13'
                                        )  # JOINS department table to staff at department ID
            print(f'dept is >>>>>>>>>> {dept}')
            print('\n')
            print('\n')

            
            # dept is a dictionary that looks like this:
            # {"department_name": 'cccc ccc', "location": 'ddddd'}
            pp_staff_dim_table[i]["department_name"] = dept.get("department_name") 
            pp_staff_dim_table[i]["location"] = dept.get("location")
        # except Exception as e:
        #     print(f"Error on iteration {i}: {e}")
        #     print('\n')
        #     print('\n')
        #     break  

    # preproc_staff_dim_table is now the 
    # finished staff dimension table. 
    # Return it:          
    return pp_staff_dim_table













# OLD CODE:
    # dim_staff = []

    # department_lookup = {  # a look up for the department table
    #     dept["department_id"]: {
    #         "department_name": dept.get("department_name"),
    #         "location": dept.get("location"),
    #     }
    #     for dept in dept_data
    # }

    # for staff in staff_data:
    #         dept = department_lookup.get(
    #             staff.get("department_id")
    #         )  # JOINS department table to staff at department ID
    #         transformed_row = {
    #             "staff_id": staff.get("staff_id"),
    #             "first_name": staff.get("first_name"),
    #             "last_name": staff.get("last_name"),
    #             "email_address": staff.get("email_address"),
    #             "department_name": dept.get("department_name"),
    #             "location": dept.get("location"),
    #         }
    #         dim_staff.append(transformed_row)