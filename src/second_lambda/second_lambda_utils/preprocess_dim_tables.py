


# def preprocess_dim_tables(table: list, kv_dict: dict):
#     """
#     This function:
#         preprocesses an unjsonified version of a table that the
#          second lambda function has read from the ingestion 
#          bucket. sometimes another utility function will 
#          transform the preprocessed table into a final 
#          dimension table. Sometimes the table that this function 
#          returns needs no further processing and is the finished 
#          dimension table.

#     Args:
#         table: a list of dictionaries. This is an unjsonified 
#          version of a table from the ingestion bucket. Each 
#          dictionary represents a table row. The number of 
#          dictionaries is the number of rows in a whole table.
#         kv_dict: a dictionary whose keys are the keys of the 
#          passed-in table list member dictionaries and their values
#          are what their equivalent keys will be in the dimension 
#          table.

#     Returns:
#         A list of dictionaries that represents a either a finished 
#          dimension table or a table to which another utility 
#          function will add key-value pairs 
#          (ie columnName-cellValue pairs) to make it a finished 
#          dimension table. Each dictionary represents a row.  
#     """
#     list_to_return = []

#     # typical kv_dict follows. keys are the keys the transformed row will have
#     # and values are the keys in the item in data whose value must be made value of key in transformed row :
#     # counterparty_dict ={
#     #    "counterparty_id": "counterparty_id",  
#     #    "counterparty_legal_name": "counterparty_legal_name",
#     #                    },
#     # so transformed_row must have 
#     # key "counterparty_id" whose value is item.get("counterparty_id")
#     # key "counterparty_legal_name" whose value is item.get("counterparty_legal_name")

#     # data is, eg, [{"counterparty_id": "1", "counterparty_legal_name": 'xxx', etc}, 
#     #           {"counterparty_id": "2", "counterparty_legal_name": 'yyy', etc}, 
#     #           {"counterparty_id": "3", "counterparty_legal_name": 'zzz', etc}, 
#     #            etc
#     #         ]


#     for item in table:
#             transformed_row = {key: item.get(value)   for key, value in kv_dict.items()   }
#             list_to_return.append(transformed_row)
#     return list_to_return

    

def preprocess_dim_tables(table: list, keys_to_cut: list):
    """
    This function:
        Removes certain keys from a list that 
        represents a table. Code had read that 
        table from the ingestion bucket and 
        unjsonified it into a python list of 
        dictionaries, where each dictionary 
        represents a table row.
        
    
    Args:
        1) table: a table in the form of a 
            python list of dictionaries, where 
            each dictionary represents a row of
            the table. The table came from the 
            ingestion bucket.
        2) keys_to_cut: a list of strings, each 
            representing a key that this function 
            will remove from each dictionary in 
            the list table.

    Returns:
        A new list that is a version of list 
         table but whose member dictionaries 
         no longer contain the keys whose 
         names are in list keys_to_cut.            
    
    """
    # make a deep copy of table:

    copy_list = [dict(item) for item in table]

    # This might work:
    # copy_list = [
    # {k: v for k, v in d.items() if k not in keys_to_cut}
    # for d in table
    #             ]
    
    for dct in copy_list:
        for key in keys_to_cut:
            dct.pop(key, None)

    # print(f'In transform_to_dim_counterparty and copy_list is >>>>>>   {copy_list}')

    return copy_list
    
