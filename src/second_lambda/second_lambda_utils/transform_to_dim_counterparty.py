from src.second_lambda.second_lambda_utils.dicts_for_dim_tables import counterparty_dict
from src.second_lambda.second_lambda_utils.preprocess_dim_tables import preprocess_dim_tables







def transform_to_dim_counterparty(counterparty_data, address_data):
    """
    This function: 
        transforms the counterpary table data as read from the 
        ingestion bucket (and converted into a python list) 
        into a counterparty dimension table.



    Args:
        counterparty_data: a list of dicts. This is the 
         counterparty table as held in the ingestion bucket 
         (but unjsonified). Each dict is a row of the 
         counterparty table. The number of dicts equals the
         number of rows in the counterparty table in the 
         ToteSys database (or ingestion bucket).
        address_data: a list of dicts. This is the address table
         table as held in the ingestion bucket (but unjsonified).
         Each dict is a row of the address table. The number of 
         dicts equals the number of rows in the address table in
         the ToteSys database (or ingestion bucket).
         This function uses this data to create a lookup table 
         (actually a lookup dictionary) from which to get 
         the address details of a counterparty as required by
         the counterparty dimension table.

    Returns:
        A python list of dictionaries that is the counterparty
        dimension table.         

    """


    # preproc_cp_dim_table looks like this:
    # [  {"counterparty_id": 1,  "counterparty_legal_name": 'aaaa aaaaa'},
    #  {"counterparty_id": 2,  "counterparty_legal_name": 'bbbbb bbbbb'},
    #  {"counterparty_id": 3,  "counterparty_legal_name": 'ccc ccccccc'},
    #   etc
    #]
    preproc_cp_dim_table = preprocess_dim_tables(counterparty_data, counterparty_dict)

    # make a look-up dictionary that contains data from the
    # address table. the dictionary will look like this:
    # {
    # 1: {"address_line_1": 'aaa, aaa, aaaaa', "address_line_2": 'bbb, bbb, bbbbb', 'district': 'cccc' }, etc},
    # 2: {"address_line_1": 'ddd, ddd, ddddd', "address_line_2": 'eee, eee, eeeee', 'district': 'ffff' }, etc},
    # 3: {"address_line_1": 'ggg, ggg, ggggg',  etc},
    # etc
    # }
    address_lookup = {
        address["address_id"]: {
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



    # For every dictionary in list preproc_cp_dim_table
    # add the appropriate key value pairs: 
    for i in range(len(preproc_cp_dim_table)):
        address = address_lookup.get(
                counterparty_data[i].get("legal_address_id")
                                    )  # JOINS address table to counterparty at address ID
        
        preproc_cp_dim_table[i]["counterparty_legal_address_line_1"] = address.get("address_line_1"),
        preproc_cp_dim_table[i]["counterparty_legal_address_line_2"] = address.get("address_line_2"),
        preproc_cp_dim_table[i]["counterparty_legal_district"] = address.get("district"),
        preproc_cp_dim_table[i]["counterparty_legal_city"] = address.get("city"),
        preproc_cp_dim_table[i]["counterparty_legal_postal_code"] = address.get("postal_code"),
        preproc_cp_dim_table[i]["counterparty_legal_country"] = address.get("country"),
        preproc_cp_dim_table[i]["counterparty_legal_phone_number"] = address.get("phone"),


    # preproc_cp_dim_table is 
    # now the finished 
    # counterparty dimension 
    # table. Return it:
    return preproc_cp_dim_table





    # OLD CODE:
    # for counterparty in counterparty_data:
    #     address = address_lookup.get(
    #             counterparty.get("legal_address_id")
    #                                 )  # JOINS address table to counterparty at address ID
    #     transformed_row = {
    #             "counterparty_id": counterparty.get("counterparty_id"),
    #             "counterparty_legal_name": counterparty.get("counterparty_legal_name"),
    #             "counterparty_legal_address_line_1": address.get("address_line_1"),
    #             "counterparty_legal_address_line_2": address.get("address_line_2"),
    #             "counterparty_legal_district": address.get("district"),
    #             "counterparty_legal_city": address.get("city"),
    #             "counterparty_legal_postal_code": address.get("postal_code"),
    #             "counterparty_legal_country": address.get("country"),
    #             "counterparty_legal_phone_number": address.get("phone"),
    #                      }
    #     dim_counterparty.append(transformed_row)