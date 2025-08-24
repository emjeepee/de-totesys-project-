





def test_fn(cols, vals, table_name):
    # convert ['xxx', 'yyy', 'zzz']     ->  '(xxx, yyy, zzz)' 
    # and     ['1', 'NULL', 'sausages'] ->  '('1',  NULL, 'turnip')'

    cols_str = vals_str = ''

    for i in range(len(cols)):
        cols_str += f'{cols[i]}, '
        if vals[i] is not 'NULL':
            vals_str += f"'{vals[i]}', "
        else:
            vals_str += f"{vals[i]}, "            

    cols_str = '(' + cols_str[:-2] + ')'
    vals_str = '(' + vals_str[:-2] + ')'

    # Make a string like this:
    # 'INSERT INTO sales_order (xxx, yyy, zzz) 
    # VALUES ('1',  'NULL', 'turnip')

    sql_query_str = f"INSERT INTO {table_name} {cols_str} VALUES {vals_str};"

    return sql_query_str        

#   cols_str, vals_str = ", ".join(f"{c}" for c in cols), ", ".join(f"'{v}'" for v in vals  )        

    









# for the MVP these are the 
# tables you need to create:
# fact_sales_order
# dim_staff  
# dim_location
# dim_design
# dim_date
# dim_currency
# dim_counterparty


# Table                        Tables with association:
# fact_sales_order          
# dim_staff                     purchase_order, sales_order
# dim_location                  department    
# dim_design                    sales_order
# dim_date                      
# dim_currency                  payment, purchase_order, sales_order
# dim_counterparty              purchase_order, payment, sales_order







# These are the table names and their column names:
# {
# '_prisma_migrations': ['id', 'checksum', 'finished_at', 'migration_name', 'logs', 'rolled_back_at', 'started_at', 'applied_steps_count'], 
# 'counterparty': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 
# 'address': ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'], 
# 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated'], 
# 'purchase_order': ['purchase_order_id', 'created_at', 'last_updated', 'staff_id', 'counterparty_id', 'item_code', 'item_quantity', 'item_unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 
# 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 
# 'payment_type': ['payment_type_id', 'payment_type_name', 'created_at', 'last_updated'], 
# 'payment': ['payment_id', 'created_at', 'last_updated', 'transaction_id', 'counterparty_id', 'payment_amount', 'currency_id', 'payment_type_id', 'paid', 'payment_date', 'company_ac_number', 'counterparty_ac_number'], 
# 'transaction': ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated'], 
# 'design': ['design_id', 'created_at', 'design_name', 'file_location', 'file_name', 'last_updated'], 
# 'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 
# 'currency': ['currency_id', 'currency_code', 'created_at', 'last_updated']
# }







counterparty_dict ={
"counterparty_id": "counterparty_id",
"counterparty_legal_name": "counterparty_legal_name",
                    }

currency_dict ={
                "currency_id": "currency_id",
                "currency_code": "currency_code"
               }


design_dict = {
                "design_id": "design_id",
                "design_name": "design_name",
                "file_location": "file_location",
                "file_name": "file_name"
            }

location_dict = {
                "location_id": "address_id",
                "address_line_1": "address_line_1",
                "address_line_2": "address_line_2",
                "district": "district",
                "city": "city",
                "postal_code": "postal_code",
                "country": "country",
                "phone": "phone",
                }

staff_dict = {
                "new_staff_id": "old_staff_id",
                "new_first_name": "old_first_name",
                "new_last_name": "old_last_name",
                "new_email_address": "old_email_address"
            }


test_staff_table = [
                {"old_staff_id": 1,
                "old_first_name": "Mukund",
                "old_last_name": "Pandit",
                "old_email_address": "mp@test.com"},

                {"old_staff_id": 2,
                "old_first_name": "Madonna",
                "old_last_name": "Cipriati",
                "old_email_address": "madonna@test.com"},

                {"old_staff_id": 3,
                "old_first_name": "Nigel",
                "old_last_name": "Tuffnell",
                "old_email_address": "nigel@test.com"},

]




