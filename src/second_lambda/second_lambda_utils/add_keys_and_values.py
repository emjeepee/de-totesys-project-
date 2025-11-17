





def add_keys_and_values(dict_1, dict_2, key_pairs):
    """
    This function:
        1) Gets called by function 
        transform_to_dim_counterparty

        2) takes the values of 
        specific keys in dict_2 and 
        makes them the values of keys 
        in dict_1

    Args:
        dict_1: a dictionary that 
        must take on certain values
        for specific keys         

        dict_2: thedictionary that 
        will supply the values for 
        the keys of dict_1

        key_pairs: a list of tuples,
        each tuple taking this form:
        (key_for_dict_1, key_for_dict_2)   

    
    """

    for key_pair in key_pairs:
        dict_1[key_pair[0]] = dict_2[key_pair[1]]        











    # dict_1["counterparty_legal_address_line_1"]  = dict_2["address_line_1"]
    # dict_1["counterparty_legal_address_line_2"] = dict_2["address_line_2"]
    # dict_1["counterparty_legal_district"] = dict_2["district"]
    # dict_1["counterparty_legal_legal_city"] = dict_2["city"]
    # dict_1["counterparty_legal_legal_postal_code"] = dict_2["postal_code"]
    # dict_1["counterparty_legal_country"] = dict_2["country"]
    # dict_1["counterparty_legal_phone_number"] = dict_2["phone"]