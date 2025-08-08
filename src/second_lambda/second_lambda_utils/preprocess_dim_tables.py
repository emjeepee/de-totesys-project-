from src.second_lambda.second_lambda_utils.dicts_for_dim_tables import counterparty_dict, currency_dict, design_dict, dict_location, dim_staff


def preprocess_dim_tables(table_lst_of_dicts: list, kv_dict: dict):
    """
    This function:
        preprocesses an unjsonified version of a table that the
         second lambda function has read from the ingestion 
         bucket. Another utility function will transform
         the preprocessed table into a final dimension table. 
         Sometimes the preprocessed table that this function 
         returns needs no further processing and is the finished 
         dimension table.

    Args:
        data: a list of dictionaries, each dictionary representing
         a table row. The number of dictionaries is the number 
         of rows in a whole table.
        kv_dict: a dictionary whose keys are the keys of the 
         passed-in table list member dictionaries and their values
         are what their equivalent keys will be in the dimension 
         table.

    Returns:
        A list of dictionaries that represents a dimension table. 
         Each dictionary represents a row. If the list does not 
         represent a final dimension table then it will be 
         missing some key-value pairs (columnName-cellValue pairs)
         that another utility function will furnish.         
    """
    list_to_return = []

    # typical kv_dict follows. keys are the keys the transformed row will have
    # and values are the keys in the item in data whose value must be made value of key in transformed row :
    # counterparty_dict ={
    #    "counterparty_id": "counterparty_id",  
    #    "counterparty_legal_name": "counterparty_legal_name",
    #                    },
    # so transformed_row must have 
    # key "counterparty_id" whose value is item.get("counterparty_id")
    # key "counterparty_legal_name" whose value is item.get("counterparty_legal_name")

    # data is, eg, [{"counterparty_id": "1", "counterparty_legal_name": 'xxx', etc}, 
    #           {"counterparty_id": "2", "counterparty_legal_name": 'yyy', etc}, 
    #           {"counterparty_id": "3", "counterparty_legal_name": 'zzz', etc}, 
    #            etc
    #         ]


    for item in table_lst_of_dicts:
            transformed_row = {key: item.get(value)   for key, value in kv_dict.items()   }
            list_to_return.append(transformed_row)
    return list_to_return

    