from src.second_lambda.second_lambda_utils.preprocess_dim_tables import preprocess_dim_tables



def transform_to_dim_staff(staff_data, dept_data):
    """
    This function:
        transforms the staff table data that came from the 
         ingestion bucket and that code earlier converted 
         into a python list into a staff dimension table.
         In doing so this function creates a lookup table
         for department data. This is in the form of a
         dictionary.

    Args:
        staff_data: a list of dictionaries that represents 
         the staff table and that came from the ingestion 
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
    # The department table has these column names:
    # 'department_id',  'department_name',  'location',  'manager',  'created_at',  'last_updated' 
    # Make a lookup dictionary that looks like this:
    # {
    #  1: {"department_name": 'aaaa aaa', "location": 'bbbbb'},
    #  2: {"department_name": 'cccc ccc', "location": 'ddddd'},
    # etc
    # }
    department_lookup = {  # a look up for the department table
        dept["department_id"]: {
            "department_name": dept.get("department_name"),
            "location": dept.get("location"),
                                }
        for dept in dept_data
                        }
    
    # create a preprocessed staff dimension table:
    pp_staff_dim_table = preprocess_dim_tables(staff_data, ['created_at', 'last_updated'])

    # add keys to the preprocessed staff dimension table
    # to make the final staff dimension table:
    for i in range(len(pp_staff_dim_table)):
        # department_lookup is a dictionary that looks like this:
            # {
            #  1: {"department_name": 'aaaa aaa', "location": 'bbbbb'},
            #  2: {"department_name": 'cccc ccc', "location": 'ddddd'},
            # etc
            # }
        dept = department_lookup.get(
                    staff_data[i].get("department_id") # 13
                                    )  # JOINS department table to staff at department ID
        
        # dept is a dictionary that looks like this:
        # {"department_name": 'cccc ccc', "location": 'ddddd'}
        pp_staff_dim_table[i]["department_name"] = dept.get("department_name")
        pp_staff_dim_table[i]["location"] = dept.get("location")
          

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