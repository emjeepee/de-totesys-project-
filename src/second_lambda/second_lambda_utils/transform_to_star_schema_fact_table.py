from src.second_lambda.second_lambda_utils.dt_splitter import dt_splitter


def transform_to_star_schema_fact_table(table_data):
    """
    Transforms the sales order data into a fact table
    Takes the table name (should be "sales_order") and the table data (a list of dictionaries)
    Returns a list of dictionaries representing the transformed data
    """
    
    fact_sales_order = []


    for row in table_data:
        dt_created = row.get("created_at")
        if dt_created:
                split_dt = dt_splitter(dt_created)
                created_date = split_dt["date"]
                created_time = split_dt["time"]
        else:
                created_time = None
                created_date = None

        dt_updated = row.get("last_updated")
        
        if dt_updated:
                split_dt = dt_splitter(dt_updated)
                last_updated_date = split_dt["date"]
                last_updated_time = split_dt["time"]

        transformed_row = {
                "sales_order_id": row.get("sales_order_id"),
                "created_date": created_date,
                "created_time": created_time,
                "last_updated_date": last_updated_date,
                "last_updated_time": last_updated_time,
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
        
    fact_sales_order.append(transformed_row)
    return fact_sales_order
