from datetime import datetime, date, time 



def transform_to_star_schema_fact_table(table_data):
    """
    This function:
        1) Converts the sales-order table
            (read from the ingestion 
            bucket and in the form of a 
            list of dictionaries) into 
            the facts sales-order table.

    Args:
        table_data: the sales_order 
        table from the ingestion 
        bucket. This is a list of 
        dictionaries. 
        
    Returns:
        a list of dictionaries that 
        is the sales_order fact table.
    """
    
    # fact_sales_order table columns
    # in the warehouse and the SQL
    # types of their values:

    #    col               value's SQL type                  other 
    # sales_record_id         SERIAL          # -- make in INSERT statement -- #                          
    # sales_order_id          int                                       
    # created_date            date                                      (from 'created_at')
    # created_time            time                                      (from 'created_at')
    # last_updated_date       date                                      (from 'last_updated')
    # last_updated_time       time                                      (from 'last_updated')
    # sales_staff_id          int                                       (same as staff_id?)
    # counterparty_id         int                                       
    # units_sold              int                                       
    # unit_price            numeric(10,2)                           
    # currency_id             int                                       
    # design_id               int                                       
    # agreed_payment_date     date                           
    # agreed_delivery_date    date                            
    # agreed_delivery_        int
    #          location_id                                    



    # the sales_orders table as retrieved from
    # the ingestion bucket looks like this:
    #                     [
    #          {'sales_order_id': 15647, 
    #         'created_at': datetime(2025, 8, 13, 9, 47, 9, 901000), 
    #         'last_updated': datetime(2025, 8, 13, 9, 47, 9, 901000),  
    #         'design_id': 648,  
    #         'staff_id': 19,  
    #         'counterparty_id': 14, 
    #         'units_sold': 36692, 
    #         'unit_price': Decimal('2.40'), 
    #         'currency_id': 2, 
    #         'agreed_delivery_date':   # '2025-08-20', 
    #         'agreed_payment_date': '2025-08-16', 
    #         'agreed_delivery_location_id': 11},
    #                     ]



    fact_sales_order = []

    for row in table_data:
        # For each row of the table
        # get the created_at value
        # (a datetime object) and 
        # convert to datetime date 
        # and datetime time objects. 
        # Do similar for other  
        # values:

        #---------------
        # DELETE THIS LATER:
        iso_date = row.get("created_at")
        type_var = type(iso_date)
        print(f"MY INFO: in transform_to_star_schem_fact_table() and the value of key created_at is {iso_date} and the type is {type_var} ")
        # 2025-11-14T15:17:08 and the type is <class 'str'> 
        #---------------


        iso_cr_date = row.get("created_at")
        dt_created = datetime.fromisoformat(iso_cr_date)
        dt_created_time = dt_created.time() # extract time only
        dt_created_date = dt_created.date() # extract date only

        iso_up_date = row.get("created_at")
        dt_updated = datetime.fromisoformat(iso_up_date)
        dt_updated_time = dt_updated.time() # extract time only
        dt_updated_date = dt_updated.date() # extract date only

        apd_str = row.get("agreed_payment_date") # eg '2025-08-16'
        dt_apd = datetime.strptime(apd_str, "%Y-%m-%d").date()

        add_str = row.get("agreed_delivery_date") # eg '2025-09-20'
        dt_add = datetime.strptime(add_str, "%Y-%m-%d").date()

        up_dec = row.get("unit_price")
        up_form = f"{float(up_dec):.2f}" # eg '3.56'
        

        facts_table_row = {
            # sales_record_id -> create in INSERT string in third lambda
            "sales_order_id": row.get("sales_order_id"),    # in warehouse is int NN
            "created_date": dt_created_date,                # in warehouse is date NN 
            "created_time": dt_created_time,                # in warehouse is time NN 
            "last_updated_date": dt_updated_date,           # in warehouse is date NN 
            "last_updated_time": dt_updated_time,           # in warehouse is time NN 
            "sales_staff_id": row.get("staff_id"),          # in warehouse is int NN
            "counterparty_id": row.get("counterparty_id"),  # in warehouse is int NN
            "units_sold": row.get("units_sold"),            # in warehouse is int NN
            "unit_price": up_form,                          # in warehouse is numeric(10,2)
            "currency_id": row.get("currency_id"),          # in warehouse is int NN
            "design_id": row.get("design_id"),              # in warehouse is int NN
            "agreed_payment_date": dt_apd,                  # in warehouse is date NN 
            "agreed_delivery_date": dt_add,                 # in warehouse is date NN 
            "agreed_delivery_location_id": \
                row.get("agreed_delivery_location_id"),     # in warehouse is int NN
                        }
       
        fact_sales_order.append(facts_table_row)
    return fact_sales_order
