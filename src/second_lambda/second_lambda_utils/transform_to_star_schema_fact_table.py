from datetime import datetime, date, time



def transform_to_star_schema_fact_table(table_data):
    """
    This function:
        1) Converts the sales-order table
            (retrieved from the ingestion 
            bucket and converted into a 
            Python list of dictionaries)
            into the facts sales order 
            table.

    Args:
        table_data: the sales_order table 
         from the ingestion bucket 
         converted into a Python list of 
         dictionaries. 
        
    Returns:
        a Python list of dictionaries that 
         is the sales_order fact table.

    """
    
    # The following shows the columns
    # of the fact_sales_order table
    # in the warehouse and the SQL
    # types of their values:
    #    col               value's SQL type      eg
    # created_date            date            2024-01-01
    # created_time            time            14:35:20  
    # last_updated_date       date            2024-01-01
    # last_updated_time       time            14:35:20  
    # agreed_payment_date     date            2024-01-01  
    # agreed_delivery_date    date            2024-01-01  
    # unit_price            numeric(10,2)       2.45  
    #    All the rest are ints


    # the sales_orders table as retrieved from
    # the ingestion bucket looks like this:
            #             [
            #  {'sales_order_id': 15647, 'created_at': datetime(2025, 8, 13, 9, 47, 9, 901000), 'last_updated': datetime(2025, 8, 13, 9, 47, 9, 901000),  'design_id': 648,  'staff_id': 19,  'counterparty_id': 14, 'units_sold': 36692, 'unit_price': Decimal('2.40'), 'currency_id': 2, 'agreed_delivery_date': '2025-08-20', 'agreed_payment_date': '2025-08-16', 'agreed_delivery_location_id': 11},
            #  {'sales_order_id': 15648, 'created_at': datetime(2025, 9, 13, 9, 47, 9, 901000), 'last_updated': datetime(2025, 9, 13, 9, 47, 9, 901000),  'design_id': 649,  'staff_id': 19,  'counterparty_id': 12, 'units_sold': 36692, 'unit_price': Decimal('2.40'), 'currency_id': 2, 'agreed_delivery_date': '2025-09-20', 'agreed_payment_date': '2025-09-16', 'agreed_delivery_location_id': 12},
            #    etc
            #             ]



    fact_sales_order = []

    for row in table_data:
        # For each row of the table
        # get the created_at value
        # (a datetime object) and 
        # convert to datetime date 
        # and datetime time objects. 
        # Do similar for other  
        # values:
        dt_created = row.get("created_at")
        dt_created_time = dt_created.time() # extract time only
        dt_created_date = dt_created.date() # extract date only

        dt_updated = row.get("last_updated")
        dt_updated_time = dt_updated.time() # extract time only
        dt_updated_date = dt_updated.date() # extract date only

        apd_str = row.get("agreed_payment_date") # eg '2025-08-16'
        dt_apd = datetime.strptime(apd_str, "%Y-%m-%d").date()

        add_str = row.get("agreed_delivery_date") # eg '2025-09-20'
        dt_add = datetime.strptime(add_str, "%Y-%m-%d").date()

        up_dec = row.get("unit_price")
        up_form = f"{up_dec:.2f}" # eg '3.56'
        

        facts_table_row = {
                "sales_order_id": row.get("sales_order_id"),    # type is int
                "created_date": dt_created_date,                # type is datetime.date 
                "created_time": dt_created_time,                # type is datetime.time 
                "last_updated_date": dt_updated_date,           # type is datetime.date 
                "last_updated_time": dt_updated_time,           # type is datetime.time 
                "sales_staff_id": row.get("staff_id"),          # type is int
                "counterparty_id": row.get("counterparty_id"),  # type is int
                "units_sold": row.get("units_sold"),            # type is int
                "unit_price": up_form,                          # type is string
                "currency_id": row.get("currency_id"),          # type is int
                "design_id": row.get("design_id"),              # type is int
                "agreed_payment_date": dt_apd,                  # type is datetime.date 
                "agreed_delivery_date": dt_add,                 # type is datetime.date 
                "agreed_delivery_location_id": \
                    row.get("agreed_delivery_location_id"),     # type is int
                        }
       
        fact_sales_order.append(facts_table_row)
    return fact_sales_order
