from src.second_lambda.second_lambda_utils.preprocess_dim_tables import preprocess_dim_tables




def transform_to_dim_location(address_data: list):
    """
    This function:
        transforms the address table data as read from the 
        ingestion bucket (and converted into a python list) 
        into a location dimension table.

    Args:
        address_data: a list of dictionaries that represents 
         the address table. Each dictionary represents a row
         (and its key-value pairs represent columnName-cellValue
         pairs). This list is the unjsonified address table 
         that the ingestion bucket stored.

    Returns:
        A list of dictionaries that is the location dimension 
        table.    
    """
    # OLD CODE:
    # dim_location = []

    # for location in location_data:
        
    #         transformed_row = {
    #             "location_id": location.get("address_id"),
    #             "address_line_1": location.get("address_line_1"),
    #             "address_line_2": location.get("address_line_2"),
    #             "district": location.get("district"),
    #             "city": location.get("city"),
    #             "postal_code": location.get("postal_code"),
    #             "country": location.get("country"),
    #             "phone": location.get("phone"),
    #         }
    #         dim_location.append(transformed_row)
    # return dim_location

    # create the final location 
    # dimension table:
    location_dim_table = preprocess_dim_tables(address_data, ['created_at', 'last_updated'])
        # In each dict in the list 
        # change the name of key  
        # "address_id" to
        # "location_id":
    for dict in location_dim_table:
        dict["location_id"] = dict.pop("address_id")

    return location_dim_table
    

