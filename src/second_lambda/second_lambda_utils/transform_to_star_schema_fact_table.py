

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
    #    All the rest are ints

    fact_sales_order = []


    for row in table_data:
        # For each row of the table
        # get the created_at value
        # (a datetime object) and 
        # convert to datetime date 
        # and datetime time objects. 
        # Do the same for other  
        # values:
        dt_created = row.get("created_at")
        dt_created_time = dt_created.time() # extract time only
        dt_created_date = dt_created.date() # extract date only

        dt_updated = row.get("last_updated")
        dt_updated_time = dt_updated.time() # extract time only
        dt_updated_date = dt_updated.date() # extract date only

        dt_apd = row.get("agreed_payment_date")
        dt_apd = dt_apd.date()  # extract date only

        dt_add = row.get("agreed_delivery_date")
        dt_add = dt_add.date()  # extract date only


        table_row = {
                "sales_order_id": row.get("sales_order_id"),
                "created_date": dt_created_date,
                "created_time": dt_created_time,
                "last_updated_date": dt_updated_date,
                "last_updated_time": dt_updated_time,
                "sales_staff_id": row.get("staff_id"),
                "counterparty_id": row.get("counterparty_id"),
                "units_sold": row.get("units_sold"),
                "unit_price": row.get("unit_price"),
                "currency_id": row.get("currency_id"),
                "design_id": row.get("design_id"),
                "agreed_payment_date": dt_apd,
                "agreed_delivery_date": dt_add,
                "agreed_delivery_location_id": row.get("agreed_delivery_location_id"),
                        }
        
    fact_sales_order.append(table_row)
    return fact_sales_order
