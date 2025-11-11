from .preprocess_dim_tables import preprocess_dim_tables







def transform_to_dim_counterparty(counterparty_data, address_data):
    """
    This function: 
        1) transforms the list 
            version of the 
            counterparty table from 
            the ingestion bucket 
            into a counterparty 
            dimension table.
        2) uses the address table 
            (address_data) to create 
            a lookup table in the 
            form of a dictionary.
        3) uses the lookup table to 
            get the address details of 
            a counterparty to be able 
            to create the counterparty 
            dimension table.

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
        dimension table.         

    """
    # the counterparty table from the ingestion 
    # bucket is a list that looks like this: 
    # ['counterparty_id': 'xxx', 
    # 'counterparty_legal_name': 'xxx', 
    # 'legal_address_id': 'xxx',           DON"T NEED
    # 'commercial_contact': 'xxx',         DON"T NEED
    # 'delivery_contact': 'xxx',           DON"T NEED
    # 'created_at': 'xxx',                 DON"T NEED
    # 'last_updated': 'xxx']               DON"T NEED

# DO NEED:
# "counterparty_legal_address_line_1"   COMES FROM LOOKUP
# "counterparty_legal_address_line_2"   COMES FROM LOOKUP
# "counterparty_legal_district"         COMES FROM LOOKUP
# "counterparty_legal_city"             COMES FROM LOOKUP
# "counterparty_legal_postal_code"      COMES FROM LOOKUP   
# "counterparty_legal_country"          COMES FROM LOOKUP
# "counterparty_legal_phone_number"     COMES FROM LOOKUP

    # To create the dimension 
    # counterparty table do this:
    # 1) remove the unneeded columns
    #    and their cell values from
    #    each row of the table in
    #    counterparty_data. Call 
    #    the resulting table 
    #    pp_cp_dim_table
    #    (counterparty_data is a list 
    #    of dicts, each dict a row,
    #    the key-vlaue pairs of which 
    #    are columnname-cellvalue 
    #    pairs) 
    # 2) make a look-up dictionary
    #    from the address table. Its
    #    keys will be address IDs.
    #    The values of the keys are 
    #    dicts whose key-value pairs 
    #    are columnname-cellvalue 
    #    pairs.
    #    The lookup will look like 
    #    this:
    #    {
    #       '30': {"address_line_1": 'aaa, aaa, aaaaa', "address_line_2": 'bbb, bbb, bbbbb', 'district': 'cccc' }, etc},
    #       '31': {"address_line_1": 'ddd, ddd, ddddd', "address_line_2": 'eee, eee, eeeee', 'district': 'ffff' }, etc},
    #       '32': {"address_line_1": 'ggg, ggg, ggggg',  etc},
    #           etc
    #    }

    # 3) For every row dictionary in 
    #    list pp_cp_dim_table add the 
    #    appropriate key value pairs. 

 
    # 1): 
    pp_cp_dim_table = preprocess_dim_tables(counterparty_data, ['legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'])

    # 2):
    address_lookup = {
        str(address["address_id"]): {
            "address_line_1": address.get("address_line_1"),
            "address_line_2": address.get("address_line_2"),
            "district": address.get("district"),
            "city": address.get("city"),
            "postal_code": address.get("postal_code"),
            "country": address.get("country"),
            "phone": address.get("phone"),
                                    }
        for address in address_data
                    }

    # 3):
    for i in range(len(pp_cp_dim_table)):
        address = address_lookup.get( # {"address_line_1": 'ddd, ddd, ddddd', "address_line_2": 'eee, eee, eeeee', 'district': 'ffff' }, etc},
                str(counterparty_data[i].get("legal_address_id")) # in test function '30', '31' and '32'
                                    )  # Joins address table to counterparty at address ID

        pp_cp_dim_table[i]["counterparty_legal_address_line_1"] = address.get("address_line_1")
        pp_cp_dim_table[i]["counterparty_legal_address_line_2"] = address.get("address_line_2")
        pp_cp_dim_table[i]["counterparty_legal_district"] = address.get("district")
        pp_cp_dim_table[i]["counterparty_legal_city"] = address.get("city")
        pp_cp_dim_table[i]["counterparty_legal_postal_code"] = address.get("postal_code")
        pp_cp_dim_table[i]["counterparty_legal_country"] = address.get("country")
        pp_cp_dim_table[i]["counterparty_legal_phone_number"] = address.get("phone")


    # pp_cp_dim_table is 
    # now the finished 
    # counterparty dimension 
    # table. 
    # Its column names are:
    # 'counterparty_id' 
    # 'counterparty_legal_name' 
    # "counterparty_legal_address_line_1"
    # "counterparty_legal_address_line_2"
    # "counterparty_legal_district"
    # "counterparty_legal_city"
    # "counterparty_legal_postal_code"
    # "counterparty_legal_country"
    # "counterparty_legal_phone_number"

    # Return the counterparty 
    # dimension table:
    return pp_cp_dim_table

