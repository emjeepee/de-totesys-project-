from src.first_lambda.first_lambda_utils.convert_json_to_string import convert_json_to_string
from src.first_lambda.first_lambda_utils.convert_Decimal_to_float import convert_Decimal_to_float
from src.first_lambda.first_lambda_utils.convert_datetime_to_string import convert_datetime_to_string


def convert_dt_values_to_iso(list_of_rows):
    """
    This function:
        converts 
         i)   datetime objects to ISO format strings.
         ii)  json data to strings.
         iii) 


        the member lists of list_of_rows
        to iso format.

    Args:
        list_of_rows: a list of lists. Each 
        member list represents a row of a 
        table (and contains just the cell
        values, not the column names).

    Returns:
        A version of list_of_rows where each member
        list now contains iso format time strings
        where datatime objects previously existed. 
    """
    # list_to_return = [  [ serialise_datetime(list_of_rows[i][j])         for j in range(len(list_of_rows[0])) ]   for i in range(len(list_of_rows))      ]

    
    # return list_to_return
    

# Need to convert:
# json -> string
# datetime obj -> iso  string
# Decimal to float
# None -> leave as is, convert in dataframe 
# Boolean -- in third lambda change True to TRUE in SQL string
# some dates are already in this form: '2025-08-16' -- no need to do anything here
# 
# 
# 
# 
# Python automatically converts SQL Null values in the OLTP db to None in the Python list.
# use pandas to convert None or NaN to 'Unknown' (in third lambda):
# import pandas as pd

# df = pd.DataFrame({
#     'some_column': ['A', None, 'B', None]
# })

# df['some_column'].fillna('Unknown', inplace=True)

# print(df) 


# design table cols and typical values
# [['design_id'], ['created_at'], ['design_name'], ['file_location'], ['file_name'], ['last_updated']]
# [[654, datetime.datetime(2025, 8, 12, 12, 11, 10, 73000), 'Fresh', '/Network', 'fresh-20240124-ap0b.json', datetime.datetime(2025, 8, 12, 12, 11, 10, 73000)]]

# "payment" table cols and typical values,
# [['payment_id'], ['created_at'], ['last_updated'], ['transaction_id'], ['counterparty_id'], ['payment_amount'], ['currency_id'], ['payment_type_id'], ['paid'], ['payment_date'], ['company_ac_number'], ['counterparty_ac_number']]
# [[22128, datetime.datetime(2025, 8, 13, 9, 47, 9, 901000), datetime.datetime(2025, 8, 13, 9, 47, 9, 901000), 22128, 14, Decimal('88060.80'), 2, 1, False, '2025-08-16', 36395935, 53576911]]


# "sales_order" table cols and typical values
# [['sales_order_id'], ['created_at'], ['last_updated'], ['design_id'], ['staff_id'], ['counterparty_id'], ['units_sold'], ['unit_price'], ['currency_id'], ['agreed_delivery_date'], ['agreed_payment_date'], ['agreed_delivery_location_id']]
# [[15647, datetime.datetime(2025, 8, 13, 9, 47, 9, 901000), datetime.datetime(2025, 8, 13, 9, 47, 9, 901000), 648, 19, 14, 36692, Decimal('2.40'), 2, '2025-08-20', '2025-08-16', 11]]


# "transaction" table cols and typical values
# [['transaction_id'], ['transaction_type'], ['sales_order_id'], ['purchase_order_id'], ['created_at'], ['last_updated']]
# [[22128, 'SALE', 15647, None, datetime.datetime(2025, 8, 13, 9, 47, 9, 901000), datetime.datetime(2025, 8, 13, 9, 47, 9, 901000)]]


# "sales_order" table cols and typical values
# [['sales_order_id'], ['created_at'], ['last_updated'], ['design_id'], ['staff_id'], ['counterparty_id'], ['units_sold'], ['unit_price'], ['currency_id'], ['agreed_delivery_date'], ['agreed_payment_date'], ['agreed_delivery_location_id']]
# [[15647, datetime.datetime(2025, 8, 13, 9, 47, 9, 901000), datetime.datetime(2025, 8, 13, 9, 47, 9, 901000), 648, 19, 14, 36692, Decimal('2.40'), 2, '2025-08-20', '2025-08-16', 11]]


# "counterparty" table cols and typical values
# [['counterparty_id'], ['counterparty_legal_name'], ['legal_address_id'], ['commercial_contact'], ['delivery_contact'], ['created_at'], ['last_updated']]
# [[20, 'Yost, Watsica and Mann', 2, 'Sophie Konopelski', 'Janie Doyle', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]]


# "address" table cols and typical values
# [['address_id'], ['address_line_1'], ['address_line_2'], ['district'], ['city'], ['postal_code'], ['country'], ['phone'], ['created_at'], ['last_updated']]
# [[30, '0336 Ruthe Heights', None, 'Buckinghamshire', 'Lake Myrlfurt', '94545-4284', 'Falkland Islands (Malvinas)', '1083 286132', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]


# "staff" table cols and typical values
# [['staff_id'], ['first_name'], ['last_name'], ['department_id'], ['email_address'], ['created_at'], ['last_updated']]
# [[20, 'Flavio', 'Kulas', 3, 'flavio.kulas@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]]


# "purchase_order" table cols and typical values
# [['purchase_order_id'], ['created_at'], ['last_updated'], ['staff_id'], ['counterparty_id'], ['item_code'], ['item_quantity'], ['item_unit_price'], ['currency_id'], ['agreed_delivery_date'], ['agreed_payment_date'], ['agreed_delivery_location_id']]
# [[6481, datetime.datetime(2025, 8, 13, 9, 32, 10, 122000), datetime.datetime(2025, 8, 13, 9, 32, 10, 122000), 11, 4, 'SNOAV0N', 93, Decimal('141.48'), 2, '2025-08-15', '2025-08-18', 6]]


# "department" table cols and typical values
# [['department_id'], ['department_name'], ['location'], ['manager'], ['created_at'], ['last_updated']]
# [[8, 'HR', 'Leeds', 'James Link', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]


# "currency" table cols and typical values
# [['currency_id'], ['currency_code'], ['created_at'], ['last_updated']]
# [[3, 'EUR', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]


# "payment_type" table cols and typical values
# [['payment_type_id'], ['payment_type_name'], ['created_at'], ['last_updated']]
# [[4, 'PURCHASE_REFUND', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]






