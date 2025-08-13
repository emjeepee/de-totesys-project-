from src.second_lambda.second_lambda_utils.get_latest_table import get_latest_table



def make_staff_or_cp_dim_table(
        table_name: str, table_python: list, 
        function_lookup_table, ingestion_bucket: str, 
        aux_table_name: str, s3_client):
    """
    This function:
        Makes the staff or counterparty 
         dimension table. 
        To create the staff table, this 
         function requires data from 
         the department table, which 
         it retrieves from the 
         ingestion bucket. To create 
         the counterparty dimension 
         table, this function requires 
         data from the address table, 
         which it retrieves from 
         the ingestion bucket.

    Args:
        1) table_name: the name of the 
            table (will be either 
            'staff' or 'counterparty').
        2) table_python: the staff or 
            counterparty table as 
            retrieved from the ingestion 
            bucket and converted to a 
            python list.
        3) function_lookup_table: a 
            lookup table of functions that
            do the actual creation of 
            dimension tables. 
        4) ingestion_bucket: the name of 
            the ingestion bucket
        5) aux_table_name: the name of an 
            auxilliary table this function 
            will retrieve from the 
            ingestion bucket to help it
            create either the staff or the 
            counterparty dimension table.
            Its value will be 'department' 
            when this function has to make 
            the staff dimension table and 
            'address' for the counterparty 
            dimension table.
        6) s3_client: a boto3 S3 client 
            object.

    Returns:
        The staff or counterparty 
         dimension table as a 
         Python list of dictionaries.                        
    """


    aux_python = get_latest_table(s3_client, ingestion_bucket, aux_table_name)
    dim_table = function_lookup_table[table_name](table_python, aux_python)

    return dim_table





    # if table_name == 'staff':
    #     dept_python = get_latest_table(s3_client, ingestion_bucket, 'department')
    #     dim_or_fact_table = function_lookup_table[table_name](table_python, dept_python) # will be a dimension table   
        
    # if table_name == 'counterparty':
    #     address_python = get_latest_table(s3_client, ingestion_bucket, 'address')
    #     dim_or_fact_table = function_lookup_table[table_name](table_python, address_python)  # will be a dimension table   
