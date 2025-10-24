
from .func_lookup_table import func_lookup_table
from .make_staff_or_cp_dim_table import make_staff_or_cp_dim_table


def make_dim_or_fact_table(table_name: str, table_python: list, s3_client, ingestion_bucket: str):
    """
    This function:
        Makes either a dimension table or the fact 
        table in the form of a list of dictionaries,
        where each dictionary represents a row of 
        the table.
        
    Args:
        table_name: the name of a table.
        table_python: a list that represents a table
            from the ingestion bucket (unjsonified first)
        s3_client: a boto3 S3 client object.
        ingestion_bucket: the name of the ingestion bucket.

    Returns:
        Either a dimension table or the fact table in the
        form of a list of dictionaries, where each 
        dictionary represents a row of the table. The 
        first key in each dictionary is always the primary 
        key (important for later utility function uesd by
        third lambda handler).      
    """
    if table_name == 'staff' or table_name == 'counterparty':
        # If the table name is 'staff' 
        # or 'counterparty' the creation 
        # of their dimension tables 
        # requires data from the department 
        # table and the address table, 
        # respectively:  
 
        aux_table_name = 'department' if table_name == 'staff' else 'address'           
        tbl_to_return = make_staff_or_cp_dim_table( table_name, table_python, func_lookup_table, ingestion_bucket, aux_table_name, s3_client)
        
    else:
        # If the table name is 'currency',
        # 'design', 'location' or 
        # 'sales_order'.
        # func_lookup_table is a 
        # lookup that returns
        # a functions that converts
        # a table into a dimension
        # or fact table:     
        func = func_lookup_table(table_name)
        tbl_to_return = func(table_python) # dimension table or the fact table

    return tbl_to_return

