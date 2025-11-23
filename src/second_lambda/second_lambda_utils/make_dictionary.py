




def make_dictionary(source_dict, key_pairs):
    """
    This function:
        1) creates an empty dictionary 

        2) takes the values of 
        specific keys in source_dict
        and makes them the values of 
        specific keys in a new dict.

    Args:
        source_dict: the dictionary 
        that will supply the values 
        that this function will set 
        as the values for specific 
        keys in adictionary that 
        starts off empty.

        key_pairs: a list of tuples,
        each tuple taking this form:
        (key_dict_1, key_dict_2).
        This function takes the value
        of key key_dict_2 of 
        source_dict and makes it the
        value of key key_dict_1 in a
        dictionary that starts off 
        empty .

    Returns:
        a dictionary with key-value
        pairs. Each key is the key 
        that is the first elelment 
        of one of the tuples in list
        key_pairs. The value of 
        each key comes from the 
        value of one key in 
        source_dict. 
        For example if key_pairs is
        [
        ("dest_key_1", "source_key_1"),
        ("dest_key_2", "source_key_2"),   
        ("dest_key_3", "source_key_3")
        ]
        and source_dict is:
        {
        "source_key_1": 1,
        "source_key_2": 2,
        "source_key_3": 3
        }
        Then this function returns
        {
        "dest_key_1": 1,
        "dest_key_2": 2,
        "dest_key_3": 3,
        }
        
    
    """
    new_row = {}
    for pairs in key_pairs:
        if pairs[1] in source_dict:
            new_row[pairs[0]] = source_dict[pairs[1]]

    return new_row











    # dict_1["counterparty_legal_address_line_1"]  = dict_2["address_line_1"]
    # dict_1["counterparty_legal_address_line_2"] = dict_2["address_line_2"]
    # dict_1["counterparty_legal_district"] = dict_2["district"]
    # dict_1["counterparty_legal_legal_city"] = dict_2["city"]
    # dict_1["counterparty_legal_legal_postal_code"] = dict_2["postal_code"]
    # dict_1["counterparty_legal_country"] = dict_2["country"]
    # dict_1["counterparty_legal_phone_number"] = dict_2["phone"]








# def add_keys_and_valuesTO_DUMP(table_1, table_2, key_pairs):
#     """
#     This function:
#         1) Gets called by function 
#         transform_to_dim_counterparty

#         2) takes the values of 
#         specific keys as found in the 
#         dictionaries in the list that 
#         is table_2 and makes them the 
#         values of specific keys as 
#         found in the dictionaries of 
#         the list that is in table_1

#         3) For example if a tuple in 
#         key_pairs is ("aaa", "bbb") then 
#         this function loops through the 
#         dicts of list table_1 and for 
#         each dict, finds the key "aaa"
#         and sets its value to the value 
#         of key "bbb" in each 
#         dict in list table_2.  
#         :


#     Args:
#         table_1: a list of dictionaries
#         that represents a table. Each 
#         dict is a row. The key-value 
#         pairs in each dict represent 
#         columname-cellvalue pairs of 
#         the table.
#         This list is the recipient of
#         key value pairs.

#         table_2: as table_1 but a 
#         table that will be the source 
#         of key-value pairs.

#         key_pairs: a list of tuples,
#         each tuple taking this form:
#         (a_key_for_dict_1, a_key_for_dict_2)   

    
#     """


#     pass