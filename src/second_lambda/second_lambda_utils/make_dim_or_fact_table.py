from src.second_lambda.second_lambda_utils.get_latest_table import get_latest_table
from src.second_lambda.second_lambda_utils.function_lookup_table import function_lookup_table


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
        # get latest department table in 
        # the ingestion bucket 
        # function_lookup_table below is
        # a dict each of whose keys have 
        # a value that is a function that 
        # converts a table into a dimension
        # or facts table. In the case of 
        # the staff and counterparty tables, 
        # their conversion also requires 
        # the use of the department table 
        # and the address table, 
        # respectively:
        # dim_or_facts_table as set in the two if 
        # statements below will be a dimension 
        # table only:    
    if table_name == 'staff':
        dept_python = get_latest_table(s3_client, ingestion_bucket, 'department')
        dim_or_fact_table = function_lookup_table[table_name](table_python, dept_python) # will be a dimension table   
        
    if table_name == 'counterparty':
        address_python = get_latest_table(s3_client, ingestion_bucket, 'address')
        dim_or_fact_table = function_lookup_table[table_name](table_python, address_python)  # will be a dimension table   
 
    dim_or_fact_table = function_lookup_table[table_name](table_python) # dimension table or the fact table

    return dim_or_fact_table
