from src.second_lambda.second_lambda_utils.preprocess_dim_tables import preprocess_dim_tables



def transform_to_dim_design(design_data):
    """
    This function:
        transforms the design table data as read from the 
        ingestion bucket (and converted into a python list) 
        into a design dimension table.

    Args:
        design_data: a list of dictionaries that represents 
        the design table. Each dictionary represents a row
        (and its key-value pairs represent columnName-cellValue
        pairs). This list is the unjsonified design table 
        that the ingestion bucket stored.

    Returns:
        A list of dictionaries that is the design dimension 
        table.    
    """
        # OLD CODE:
        # """
        # Transforms the design data into a dimension table
        # Takes the design data (a lists of dictionaries)
        # Returns a list of dictionaries representing the transformed data
        # """
        # dim_design = []

        # for design in design_data:
        #     try:
        #         transformed_row = {
        #             "design_id": design.get("design_id"),
        #             "design_name": design.get("design_name"),
        #             "file_location": design.get("file_location"),
        #             "file_name": design.get("file_name"),
        #         }
        #         dim_design.append(transformed_row)
        #     except Exception as error:
        #         raise Exception(f"Error processing row{design.get("location_id")}: {error}")
        # return dim_design
    design_dim_table = preprocess_dim_tables(design_data, ['created_at', 'last_updated'])


    # design_dim_table is 
    # now the finished design
    # dimension table. Return it:
    return design_dim_table