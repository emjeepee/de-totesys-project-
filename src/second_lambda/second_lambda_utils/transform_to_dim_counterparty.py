from .preprocess_dim_tables import preprocess_dim_tables
from .make_dictionary   import make_dictionary






def transform_to_dim_counterparty(counterparty_data, address_data):
    """
    This function: 
        1) transforms the 
        counterparty table that a
        another function previously 
        read from the ingestion 
        bucket into a counterparty 
        dimension table.
        
        2) achieves 1) by looping 
        through the dictionaries in 
        the counterparty table. For 
        each dictionary, this function 
        finds the value of the key 
        'legal_address_id' and finds 
        the dictionary in the address 
        table that has the same value 
        for its key 'address_id'.
        This function then creates new
        keys in the counterparty table,
        setting the values of those 
        keys to the values of specific 
        keys in the address table.
        For example: a this function 
        will create key 
        "counterparty_legal_address_line_1"
        in the dictionary in the 
        counterparty table and set its 
        value to the value of key 
        "address_line_1" in the 
        dictionary from the address 
        table.
        This function also ensures 
        that the counterparty dimension
        table it creates contains no 
        unneeded data from the 
        counterparty table.


    Args:
        counterparty_data: a list 
        of dicts. This is the 
        the counterparty table 
        from the ingestion bucket. 
        Each dict is a row.

        address_data: a list of 
        dicts. This is the 
        address table from the 
        ingestion bucket. Each dict 
        is a row. 

    Returns:
        A list of dictionaries 
        that is the counterparty 
        dimension table, each 
        dict representing a row.         

    """

    # counterparty_data looks like this: 
    #   [{
    # 'counterparty_id': 'xxx', 
    # 'counterparty_legal_name': 'xxx', 
    # 'legal_address_id': 'xxx',           DELETE THIS
    # 'commercial_contact': 'xxx',         DELETE THIS
    # 'delivery_contact': 'xxx',           DELETE THIS
    # 'created_at':     'xxx',             DELETE THIS
    # 'last_updated': 'xxx'                DELETE THIS
    #   }]


# Add the following columns, all
# of whose values come from the 
# address table:
# "counterparty_legal_address_line_1"
# "counterparty_legal_address_line_2"
# "counterparty_legal_district"      
# "counterparty_legal_city"          
# "counterparty_legal_postal_code"   
# "counterparty_legal_country"       
# "counterparty_legal_phone_number"  



    # To create the dimension 
    # counterparty table do this:
    # 1) for every row (dict) in 
    #    pp_cp_dim_table look at its 
    #    value for counterparty_id 
    #    and find the row (dict) in 
    #    address_data that has an 
    #    identical value for 
    #    'legal_address_id'. Then 
    #    grab the necessary values 
    #    from that row of address_data 
    #    and make each the value of 
    #    an appropriate new key in   
    #    the row in pp_cp_dim_table.
    # 2) remove the unneeded columns
    #    and their cell values from
    #    each row of the table in
    #    counterparty_data so that its 
    #    column names are now:
    #    'counterparty_id' 
    #    'counterparty_legal_name' 
    #    "counterparty_legal_address_line_1"
    #    "counterparty_legal_address_line_2"
    #    "counterparty_legal_district"
    #    "counterparty_legal_city"
    #    "counterparty_legal_postal_code"
    #    "counterparty_legal_country"
    #    "counterparty_legal_phone_number"

    
 
    # 1):
    # Get the value of key 
    key_pairs = [
    ("counterparty_legal_address_line_1", "address_line_1"),
    ("counterparty_legal_address_line_2", "address_line_2"),
    ("counterparty_legal_district", "district"),
    ("counterparty_legal_city", "city"),
    ("counterparty_legal_postal_code", "postal_code"),
    ("counterparty_legal_country", "country"),
    ("counterparty_legal_phone_number", "phone"),
                ]

    # for each row in the 
    # counterparty table find
    # the value of the key 
    # 'legal_address_id' and 
    # find the row in the address
    # table whose 'address_id' 
    # key has the same value:
    cp_dim_table = []
    for row_CP in counterparty_data:
        for row_ADD in address_data:
            if row_CP['legal_address_id'] \
                == row_ADD['address_id']:
                # For the row in the counterparty
                # table add new keys and values:
                new_cp_row = make_dictionary(row_ADD, key_pairs)
                new_cp_row['counterparty_id'] = row_CP['counterparty_id'] #
                new_cp_row['counterparty_legal_name'] = row_CP['counterparty_legal_name'] 
                cp_dim_table.append(new_cp_row) #

    

    return cp_dim_table



              

