
from .func_lookup_table import func_lookup_table
from .make_staff_or_cp_dim_table import make_staff_or_cp_dim_table


def make_dim_or_fact_table(table_name: str, table_python: list, s3_client, ingestion_bucket: str):
    """
    This function:
        Makes either a dimension 
        table or the fact table 
        in the form of a list of 
        dictionaries, where each 
        dictionary represents a 
        row of the table.
        
    Args:
        table_name: the name of a 
        table. Could be any of the 
        seven names, eg 
        'sales_orders' or 'design'.

        table_python: a list that 
        represents a table from the 
        ingestion bucket and that 
        looks like this:
         [{<row>}, {<row>}, etc]
         where {<row>} is, eg,
         {
           'design_id': 123, 
           'created_at': 'xxx', 
           'design_name': 'yyy', 
            etc 
         }

        s3_client: a boto3 S3 client 
        object.

        ingestion_bucket: the name 
        of the ingestion bucket.

    Returns:
        Either a dimension table 
        or the fact table in the 
        form of a list of 
        dictionaries, where each 
        dictionary represents a 
        row of the table. The 
        first key in each 
        dictionary is always the 
        primary key.      

    """

    if table_name == 'staff' or table_name == 'counterparty':
        # If the table name is 
        # 'staff' or 
        # 'counterparty' get data 
        # from the department 
        # table or the address 
        # table, respectively, as   
        # the creation of their 
        # dimension tables 
        # requires this extra 
        # data:
        aux_table_name = 'department' if table_name == 'staff' else 'address'           
        tbl_to_return = make_staff_or_cp_dim_table( table_name, 
                                                   table_python, 
                                                   ingestion_bucket, 
                                                   aux_table_name, 
                                                   s3_client)
        
    else:
        # If the table name is 
        # 'currency', 'design' or 
        # 'location',
        # func_lookup_table 
        # returns a function 
        # that converts the table 
        # into the appropriate 
        # dimension table.
        # If the table name is  
        # 'sales_order' the 
        # function returned 
        # converts the table into 
        # the fact table:
        func = func_lookup_table(table_name)
        tbl_to_return = func(table_python) # dimension table or the fact table

    return tbl_to_return

