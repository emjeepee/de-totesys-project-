from decimal import Decimal
from datetime import datetime




def test_fn(val):
    print(f"Outside if statements and val is of type {type(val)}")    
    

    
    # Don't change ints:    
    if isinstance(val, int):
        print(f'Inside the int if statement')
        return val


    # run of spaces or '' -> 'no data'
    if isinstance(val, str):
        if bool(re.fullmatch(r" *", val)):
            return 'no data'
        else: # don't change other types of string:
            return val
        


        # None -> 'no data';        
    if val is None:
        return 'no data'
    



    if isinstance(val, bool):
        
        if val:
            print(f"Inside if statements and val is of type {type(val)}")    
            return 'TRUE'
        else:
            print(f"Inside if statements and val is of type {type(val)}")    
            return 'FALSE'

    
test_fn(True)    
test_fn(False)    




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









