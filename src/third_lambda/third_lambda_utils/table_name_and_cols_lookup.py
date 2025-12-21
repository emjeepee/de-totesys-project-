def table_name_and_cols_lookup():

    # A dictionary in which each key
    # is a table name and its value
    # is a list of the columns of
    # that table:

    fact_sales_order_cols = [
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ]

    dim_counterparty_cols = [
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
    ]

    dim_currency_cols = ["currency_id", "currency_code", "currency_name"]

    dim_design_cols = ["design_id",
                       "design_name",
                       "file_location",
                       "file_name"]

    dim_location_cols = [
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone",
    ]

    dim_staff_cols = [
        "Staff_id",
        "first_name",
        "last_name",
        "department_id",
        "email_address" "department_name",
        "location",
    ]

    dim_date_cols = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
    ]

    cols_lookup = {
        "fact_sales_order": fact_sales_order_cols,
        "dim_counterparty": dim_counterparty_cols,
        "dim_currency": dim_currency_cols,
        "dim_design": dim_design_cols,
        "dim_location": dim_location_cols,
        "dim_staff": dim_staff_cols,
        "dim_date": dim_date_cols,
    }

    return cols_lookup
