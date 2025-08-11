from src.second_lambda.second_lambda_utils.dt_splitter import dt_splitter


def transform_to_star_schema_fact_table(table_data):
    """
    This function:
        1) 
    Transforms the sales-order data into a facts table
    Takes the table name (should be "sales_order") and the table data (a list of dictionaries)
    Returns a list of dictionaries representing the transformed data

    Args:
        table_data: a list of dictionaries that came from 
        the ingestion bucket. 
        
    Returns:



    """
    
    fact_sales_order = []


    for row in table_data:
        # For each row of the table
        # get the created_at value
        # (a datetime object) and 
        # extract a date string and 
        # a time string ina a list. 
        # Then do the same for the 
        # last_updated value:
        dt_created = row.get("created_at")
        split_created_dt = dt_splitter(dt_created) # a list

        dt_updated = row.get("last_updated")
        split_updated_dt = dt_splitter(dt_updated)  # a list

        dimension_row = {
                "sales_order_id": row.get("sales_order_id"),
                "created_date": split_created_dt['date'],
                "created_time": split_created_dt['time'],
                "last_updated_date": split_updated_dt['date'],
                "last_updated_time": split_updated_dt['time'],
                "sales_staff_id": row.get("staff_id"),
                "counterparty_id": row.get("counterparty_id"),
                "units_sold": row.get("units_sold"),
                "unit_price": row.get("unit_price"),
                "currency_id": row.get("currency_id"),
                "design_id": row.get("design_id"),
                "agreed_payment_date": row.get("agreed_payment_date"),
                "agreed_delivery_date": row.get("agreed_delivery_date"),
                "agreed_delivery_location_id": row.get("agreed_delivery_location_id"),
                        }
        
    fact_sales_order.append(dimension_row)
    return fact_sales_order
