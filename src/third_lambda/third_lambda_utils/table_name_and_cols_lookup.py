



def table_name_and_cols_lookup():

# A dictionary in which each key
# is a table name and its value 
# is a list of the columns of 
# that table:

    cols_lookup = {
    "fact_sales_order": fact_sales_order,
    "dim_counterparty": dim_counterparty,
    "dim_currency": dim_currency,
    "dim_design": dim_design,
    "dim_location": [],
    "dim_staff": [],
    "dim_date": []
                }


    fact_sales_order =  ["sales_order_id", "created_date", "created_time", "last_updated_date", "last_updated_time", "sales_staff_id", "counterparty_id", "units_sold", "unit_price", "currency_id", "design_id", "agreed_payment_date", "agreed_delivery_date", "agreed_delivery_location_id"]
    dim_counterparty = [ 
    'counterparty_id', 'counterparty_legal_name', "counterparty_legal_address_line_1", "counterparty_legal_address_line_2", "counterparty_legal_district", "counterparty_legal_city" \
    "counterparty_legal_postal_code", "counterparty_legal_country", "counterparty_legal_phone_number" ]
    dim_currency = ['currency_id', 'currency_code', 'currency_name']
    dim_design = ['design_id', 'design_name', 'file_location', 'file_name' ]