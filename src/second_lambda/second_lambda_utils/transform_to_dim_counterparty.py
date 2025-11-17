from .preprocess_dim_tables import preprocess_dim_tables
from .add_keys_and_values   import add_keys_and_values






def transform_to_dim_counterparty(counterparty_data, address_data):
    """
    This function: 
        transforms the 
         counterparty table from 
         the ingestion bucket 
         into a counterparty 
         dimension table by:
         1) getting the additional 
            data needed to make the 
            counterparty dimension 
            table from the address 
            table (address_data)
        2) removing unneeded data 
            from the counterparty 
            table.

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
    key_pairs = [
    ("counterparty_legal_address_line_1", "address_line_1"),
    ("counterparty_legal_address_line_2", "address_line_2"),
    ("counterparty_legal_district", "district"),
    ("counterparty_legal_legal_city", "city"),
    ("counterparty_legal_legal_postal_code", "postal_code"),
    ("counterparty_legal_country", "country"),
    ("counterparty_legal_phone_number", "phone"),
                ]

    for row_CP in counterparty_data:
        for row_ADD in address_data:
            if row_CP['legal_address_id'] \
                == row_ADD['address_id']:
                add_keys_and_values(row_CP, row_ADD, key_pairs)

    # 2): 
    cp_dim_table = preprocess_dim_tables(counterparty_data, 
                                            [ 'legal_address_id', 
                                             'commercial_contact', 
                                             'delivery_contact', 
                                             'created_at', 
                                             'last_updated']
                                             )

    return cp_dim_table



#=========#=========#=========#=========#=========#=========#=========#=========

    # OLD CODE
                # row_CP["counterparty_legal_address_line_1"]\
                #     = row_ADD["address_line_1"]
                # row_CP["counterparty_legal_address_line_2"]\
                #     = row_ADD["address_line_2"]
                # row_CP["counterparty_legal_district"]\
                #     = row_ADD["district"]
                # row_CP["counterparty_legal_legal_city"]\
                #     = row_ADD["city"]
                # row_CP["counterparty_legal_legal_postal_code"]\
                #     = row_ADD["postal_code"]
                # row_CP["counterparty_legal_country"]\
                #     = row_ADD["country"]
                # row_CP["counterparty_legal_phone_number"]\
                #     = row_ADD["phone"]                
