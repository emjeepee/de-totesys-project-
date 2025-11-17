from .get_latest_table      import get_latest_table
from .func_lookup_table import func_lookup_table



def make_staff_or_cp_dim_table(
        table_name: str, 
        table_python: list, 
        ingestion_bucket: str, 
        aux_table_name: str, 
        s3_client):
    """
    This function:
        1) makes either:
            i)  the staff dimension table or 
            ii) the counterparty dimension 
                table. 
        2) achieves 1)i) by creating the 
            staff table, for which this 
            function requires data from 
            the department table, which 
            it retrieves from the 
            ingestion bucket. 
        3) achieves 1)ii) by getting from 
            the address table the data 
            the counterparty dimensions 
            table requires. This function 
            gets the address table from 
            the ingestion bucket.

    Args:
        1) table_name: will be either 
            'staff' or 'counterparty'.

        2) table_python: the staff or 
            counterparty table as 
            read from the ingestion 
            bucket and converted to a 
            python list.

        3) ingestion_bucket: the name of 
            the ingestion bucket.

        4) aux_table_name: the name of an 
            auxilliary table this function 
            will read from the ingestion 
            bucket to help it create 
            either the staff or the 
            counterparty dimension table.
            The value is either:
                'department' if this 
                function has to make the 
                staff dimension table
                'address' if this function
                has to make the 
                counterparty dimension 
                table.

        5) s3_client: a boto3 S3 client 
            object.

    Returns:
        The staff or counterparty 
         dimension table as a list
         list of dictionaries.          
    """

    # aux_table_name will be either 
    # 'address' or 'department'.
    # Get that table (as a list of 
    # dictionaries) from the 
    # ingestion bucket:  
    aux_python = get_latest_table(s3_client, 
                                  ingestion_bucket, 
                                  aux_table_name) 

    # function_lookup_table() here 
    # returns one of two functions:
    # 1) transform_to_dim_staff() or 
    # 2) transform_to_dim_counterparty() 
    transform_funcn = func_lookup_table(table_name)

    # Pass in to the returned 
    # function the main table 
    # (staff or counterparty) 
    # and the auxilliary table
    # (department or address, 
    # respectively) to create
    # the required dimension 
    # table:
    dim_table = transform_funcn(table_python, aux_python)

    return dim_table




