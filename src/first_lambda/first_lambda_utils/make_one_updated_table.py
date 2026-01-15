from .get_most_recent_table_data import get_most_recent_table_data
from .update_rows_in_table import update_rows_in_table




def make_one_updated_table(table_of_updates, s3_client, bucket:str):
    """
    This function:
        1) gets called by 
            put_tables_in_ing_bucket()
    
        2) gets from the ingestion 
            bucket the most recent
            table of a certain 
            name (eg 'design') 

        3) updates the rows of that 
            table that now have new 
            field data from database 
            totesys with rows

        4) returns the updated 
            version of the table                    


    Args:
        table_of_updates: a dictionary 
            that takes this form:
            {
            'design': 
            [{<updated-row data>}, etc]
            } 
            and that represents a 
            table containing only 
            rows that contain updated    
            field data from database 
            totesys.

        s3_client: boto3 S3 client

        bucket: the name of the 
            ingestion bucket

    Returns:
        the updated table as a 
        dictionary like this:
        {
        'design': 
        [{<row>}, {<row>}, etc]
        },
        where the key is the name 
        of the table, its value is 
        a list containing dictionaries
        each of which represents a 
        row and whose key-value pairs 
        are colname-fieldvalue pairs.


    
    
    """
    table_name = list(table_of_updates.keys())[0]  # 'design'

    # From the ingestion bucket
    # get the object that holds
    # the most recently updated
    # table of name table_name.
    latest_table = get_most_recent_table_data(table_name, 
                                              s3_client, 
                                              bucket 
                                            ) # [{<old row>}, {<old row>}, ...]

    # Insert the updated rows into the
    # retrieved whole table, replacing
    # the outdated ones:
    updated_table = update_rows_in_table(table_of_updates[table_name],
                                         latest_table,
                                         table_name
                                    ) # [{<updated row>}, {<updated row>}, etc]
    
    return updated_table