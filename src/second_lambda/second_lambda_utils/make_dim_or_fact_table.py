from src.second_lambda.second_lambda_utils.get_latest_table import get_latest_table
from src.second_lambda.second_lambda_utils.function_lookup_table import function_lookup_table
from src.second_lambda.second_lambda_utils.make_staff_or_cp_dim_table import make_staff_or_cp_dim_table


def make_dim_or_fact_table(table_name: str, table_python, s3_client, ingestion_bucket: str):
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
        return make_staff_or_cp_dim_table( table_name, table_python, function_lookup_table, ingestion_bucket, aux_table_name, s3_client)
    else:
        # If the table name is 'currency',
        # 'design', 'location' or 
        # 'sales_order'.
        # function_lookup_table is a 
        # dict with keys whose values
        # are functions that convert
        # a table into a dimension
        # or fact table:     
        return function_lookup_table[table_name](table_python) # dimension table or the fact table

