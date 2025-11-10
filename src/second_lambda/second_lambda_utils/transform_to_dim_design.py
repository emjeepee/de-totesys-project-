from .preprocess_dim_tables import preprocess_dim_tables



def transform_to_dim_design(design_data):
    """
    This function:
        transforms the design table's data 
        as read from the ingestion bucket 
        (and converted into a list) into
        a design dimension table.

    Args:
        design_data: a list of dictionaries 
        that represents the design table. 
        Each dictionary represents a row
        (and its key-value pairs represent 
        columnName-cellValue pairs). This 

    Returns:
        A list of dictionaries that is the 
        design dimension table. Each 
        dictionary in the list represents a 
        row and its key-value pairs represent 
        <column-Name>:<cell-Value> pairs). 
   
    """

    design_dim_table = preprocess_dim_tables(design_data, ['created_at', 'last_updated'])


    # design_dim_table is 
    # now the finished design
    # dimension table. Return it:
    return design_dim_table