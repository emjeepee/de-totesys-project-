from currency_codes import get_currency_by_code, Currency, CurrencyNotFoundError

from src.second_lambda.second_lambda_utils.preprocess_dim_tables import preprocess_dim_tables
from src.second_lambda.second_lambda_utils.make_curr_obj import make_curr_obj


def transform_to_dim_currency(currency_data):
    """
    This function:
        1) transforms the currency table data that came from the 
            ingestion bucket (and is now unjsonified) into the 
            currency dimension table. In doing so it gets rid of
            those key-value pairs that the dimension table
            does not require and adds key-value pair
            'currency_name': <name-of-currency>.

    Args:
        currency_data: a list of dictionaries that came from 
        the ingestion bucket and represents the currency 
        table. Each dictionary represents a row.

    Returns:
        A list of dictionaries that is the currency dimension 
        table.    
    """


    pp_curr_dim_table = preprocess_dim_tables(currency_data, ['created_at', 'last_updated'])
    
    for row_dict in pp_curr_dim_table: # {'currency_id': 3, 'currency_code': 'EUR'}
        curr_obj = make_curr_obj(row_dict)
        curr_name = curr_obj.name
        row_dict['currency_name'] = curr_name

    # preproc_currency_dim_table is 
    # now the finished currency 
    # dimension table. Return it:
    return pp_curr_dim_table



# Typical currency table cols and typical values
# [['currency_id'], ['currency_code'], ['created_at'], ['last_updated']]
# [[3, 'EUR', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]





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

    # return dim_currencys