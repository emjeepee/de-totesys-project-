from src.second_lambda.second_lambda_utils.dicts_for_dim_tables import currency_dict
from src.second_lambda.second_lambda_utils.preprocess_dim_tables import preprocess_dim_tables
from src.second_lambda.second_lambda_utils.make_curr_obj import make_curr_obj


def transform_to_dim_currency(currency_data):
    """
    This function:
        transforms the currency table data as read from the 
        ingestion bucket (and converted into a python list) 
        into a currency dimension table.

    Args:
        currency_data: a list of dictionaries that represents 
        the currency table. Each dictionary represents a row
        (and its key-value pairs represent columnName-cellValue
        pairs). This list is the unjsonified currency table 
        that the ingestion bucket stored.

    Returns:
        A list of dictionaries that is the currency dimension 
        table.    
    """

# OLD CODE:
    # dim_currency = []

    # for currency in currency_data:
    #     currency_obj: Currency = get_currency_by_code(
    #         currency.get("currency_code")
    #         )  # Generates a currency object based on the code

    #     currency_name = (
    #             currency_obj.name
    #         )  # Grab the name from the object made above

    #     transformed_row = {
    #             "currency_id": currency.get("currency_id"),
    #             "currency_code": currency.get("currency_code"),
    #             "currency_name": currency_name,
    #         }
    #     dim_currency.append(transformed_row)

    # return dim_currency


    preproc_currency_dim_table = preprocess_dim_tables(currency_data, currency_dict)
    for row_dict in preproc_currency_dim_table:
        row_dict['currency_name'] = make_curr_obj(row_dict).name

    # preproc_currency_dim_table is 
    # now the finished currency 
    # dimension table. Return it:
    return preproc_currency_dim_table
