from pg8000.native import Connection

from src.first_lambda.first_lambda_utils.contact_tote_sys_db import contact_tote_sys_db








def conn_to_db():
    username = "project_team_011"
    password = "WcdY1lIbOq5r853"
    database = "totesys"
    host = "nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com"
    port = 5432
    return Connection(
        username,
        database=database,
        password=password,
        host=host,
        port=port,
        ssl_context=True,
    )


def close_db(conn: Connection):
    conn.close()




# do stuff:

conn = conn_to_db()

tables_list =     [
    '_prisma_migrations', 
    'counterparty', 
    'address', 
    'department', 
    'purchase_order', 
    'staff', 
    'payment_type', 
    'payment', 
    'transaction', 
    'design', 
    'sales_order', 
    'currency'
    ]



def print_table_name_and_its_cols(conn_obj, tables_list):
    dict = {}
    for table_name in tables_list:
        query_result_2 = contact_tote_sys_db(conn_obj, 2, 'not-relevant', table_name)
        
        # Convert query_result to list 
        # of column-name strings: 
        column_names = [col[0] for col in query_result_2] # ['name', 'location', etc]

        dict[table_name] = column_names

    print(f"\n \n This is the dictionary of table names and their column names: \n {dict}")





def get_table_names(conn: Connection):
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
    """
    result = conn.run(query)
    return [row[0] for row in result]



# print(f'Names of the tables >>>> {names}')

# print_table_name_and_its_cols(conn, tables_list)

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


def test(data: list, kv_dict: dict):
    list_to_return = []
    for item in data:
        txed_dict = {key: item.get(value)   for key, value in kv_dict.items()   }
        list_to_return.append(txed_dict)
    return list_to_return

list_to_print = test(test_staff_table, staff_dict)        
    
print(f'The list is >>> {list_to_print}')

# prints:
# The list is >>> [{'new_staff_id': 1, 'new_first_name': 'Mukund', 'new_last_name': 'Pandit', 'new_email_address': 'mp@test.com'}, {'new_staff_id': 2, 'new_first_name': 'Madonna', 'new_last_name': 'Cipriati', 'new_email_address': 'madonna@test.com'}, {'new_staff_id': 3, 'new_first_name': 'Nigel', 'new_last_name': 'Tuffnell', 'new_email_address': 'nigel@test.com'}]


