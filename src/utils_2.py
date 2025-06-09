import boto3
import json
from datetime import datetime, timedelta
import boto3.exceptions
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from currency_codes import get_currency_by_code, Currency


def read_from_s3(client, bucket_name, key):
    """
    This function reads the given bucket and extracts the data at a given key

    Arguments:
    -The S3 client
    -The name of the bucket to read
    -The key of the file to read

    Returns:
    -Decoded body of the file in the S3
    """
    try:
        response = client.get_object(Bucket=bucket_name, Key=key)
        result = response["Body"].read().decode("utf-8")
        return result
    except boto3.exceptions.Boto3Error as error:
        raise f"Error reading from S3: {error}"


def upload_to_s3(client, bucket_name, key, body):
    """
    This function puts a body into a bucket

    Arguments:
    -The S3 client
    -The name of the bucket upload to
    -The key of the resulting file
    -The body to upload

    Returns:
    -Message indicating success
    """
    try:
        client.put_object(Bucket=bucket_name, Key=key, Body=body)
        return f"Successfully created {key} in {bucket_name}"
    except boto3.exceptions.Boto3Error as error:
        raise f"Error putting to S3: {error}"


def convert_json_to_python(json_data):
    """
    Converts Json data into a Python object
    Takes a json string as an argument
    Returns a Python list of dictionaries representing the rows of the data
    """
    try:
        python_data = json.loads(json_data)
        return python_data
    except Exception as error:
        raise f"Error converting json to python: {error}"


def dt_splitter(input_dt):
    """
    Splits the date from the time of a datetime
    Takes a datetimestamp as an argument
    Returns a dictionary with the keys "date" and "time"
    """
    dt = datetime.fromisoformat(input_dt)
    date = dt.date().isoformat()
    time = dt.time().isoformat()
    return {"date": date, "time": time}


def transform_to_star_schema_fact_table(table_name, table_data):
    """
    Transforms the sales order data into a fact table
    Takes the table name (should be "sales_order") and the table data (a list of dictionaries)
    Returns a list of dictionaries representing the transformed data
    """
    fact_sales_order = []
    if table_name != "sales_order":
        return fact_sales_order

    for row in table_data:
        try:
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
        except Exception as error:
            raise Exception(f"Error processing row{row.get("sales_order_id")}: {error}")
    return fact_sales_order


def transform_to_dim_staff(staff_data, dept_data):
    """
    Transforms the staff data into a dimension table
    Takes the staff data and the department data (both lists of dictionaries)
    Returns a list of dictionaries representing the transformed data
    """
    dim_staff = []

    department_lookup = {  # Allows to look at department table
        dept["department_id"]: {
            "department_name": dept.get("department_name"),
            "location": dept.get("location"),
        }
        for dept in dept_data
    }

    for staff in staff_data:
        try:
            dept = department_lookup.get(
                staff.get("department_id")
            )  # JOINS department table to staff at department ID
            transformed_row = {
                "staff_id": staff.get("staff_id"),
                "first_name": staff.get("first_name"),
                "last_name": staff.get("last_name"),
                "email_address": staff.get("email_address"),
                "department_name": dept.get("department_name"),
                "location": dept.get("location"),
            }
            dim_staff.append(transformed_row)
        except Exception as error:
            raise Exception(f"Error processing row{staff.get("staff_id")}: {error}")

    return dim_staff


def transform_to_dim_location(location_data):
    """
    Transforms the location data into a dimension table
    Takes the location data (a lists of dictionaries)
    Returns a list of dictionaries representing the transformed data
    """
    dim_location = []

    for location in location_data:
        try:
            transformed_row = {
                "location_id": location.get("address_id"),
                "address_line_1": location.get("address_line_1"),
                "address_line_2": location.get("address_line_2"),
                "district": location.get("district"),
                "city": location.get("city"),
                "postal_code": location.get("postal_code"),
                "country": location.get("country"),
                "phone": location.get("phone"),
            }
            dim_location.append(transformed_row)
        except Exception as error:
            raise Exception(
                f"Error processing row{location.get("location_id")}: {error}"
            )
    return dim_location


def transform_to_dim_design(design_data):
    """
    Transforms the design data into a dimension table
    Takes the design data (a lists of dictionaries)
    Returns a list of dictionaries representing the transformed data
    """
    dim_design = []

    for design in design_data:
        try:
            transformed_row = {
                "design_id": design.get("design_id"),
                "design_name": design.get("design_name"),
                "file_location": design.get("file_location"),
                "file_name": design.get("file_name"),
            }
            dim_design.append(transformed_row)
        except Exception as error:
            raise Exception(f"Error processing row{design.get("location_id")}: {error}")
    return dim_design


def transform_to_dim_counterparty(counterparty_data, address_data):
    """
    Transforms the counterparty data into a dimension table
    Takes the counterparty data and the address data (both lists of dictionaries)
    Returns a list of dictionaries representing the transformed data
    """
    dim_counterparty = []

    address_lookup = {  # Allows to look at address table
        address["address_id"]: {
            "address_line_1": address.get("address_line_1"),
            "address_line_2": address.get("address_line_2"),
            "district": address.get("district"),
            "city": address.get("city"),
            "postal_code": address.get("postal_code"),
            "country": address.get("country"),
            "phone": address.get("phone"),
        }
        for address in address_data
    }

    for counterparty in counterparty_data:
        try:
            address = address_lookup.get(
                counterparty.get("legal_address_id")
            )  # JOINS address table to counterparty at address ID
            transformed_row = {
                "counterparty_id": counterparty.get("counterparty_id"),
                "counterparty_legal_name": counterparty.get("counterparty_legal_name"),
                "counterparty_legal_address_line_1": address.get("address_line_1"),
                "counterparty_legal_address_line_2": address.get("address_line_2"),
                "counterparty_legal_district": address.get("district"),
                "counterparty_legal_city": address.get("city"),
                "counterparty_legal_postal_code": address.get("postal_code"),
                "counterparty_legal_country": address.get("country"),
                "counterparty_legal_phone_number": address.get("phone"),
            }
            dim_counterparty.append(transformed_row)
        except Exception as error:
            raise Exception(
                f"Error processing row {counterparty.get("counterparty_id")}: {error}"
            )

    return dim_counterparty


def transform_to_dim_date(start_date=None, end_date=None):
    """
    Transforms the date data into a dimension table
    Takes a start date (datetimestamp) and an optional end date
    Returns a list of dictionaries representing the data
    """
    try:
        if start_date is None:
            # set to 2000
            start = datetime.fromisoformat("2000-01-01").date()
        else:
            start = datetime.fromisoformat(start_date).date()
        if end_date is None:
            # set to 2030
            # end_date = datetime.today().date().isoformat()
            end = datetime.fromisoformat("2030-12-31").date()
        else:
            end = datetime.fromisoformat(end_date).date()

        # start = datetime.fromisoformat(start_date).date()
        # end = datetime.fromisoformat(end_date).date()

        if start > end:
            raise ValueError("start_date cannot be after end_date")

        dim_date = []
        current_date = start
        date_id = 1

        while current_date <= end:
            row = {
                "date_id": date_id,
                "year": current_date.year,
                "month": current_date.month,
                "day": current_date.day,
                "day_of_week": current_date.isoweekday(),
                "day_name": current_date.strftime("%A"),
                "month_name": current_date.strftime("%B"),
                "quarter": (current_date.month - 1) // 3 + 1,
            }
            dim_date.append(row)
            current_date += timedelta(days=1)
            date_id += 1
        return dim_date
    except Exception as error:
        raise Exception(f"Please ensure start and end date are valid. Error: {error}")


def transform_to_dim_currency(currency_data):
    """
    Transforms the currency data into a dimension table
    Takes the currency data (a lists of dictionaries)
    Returns a list of dictionaries representing the transformed data
    """
    dim_currency = []

    for currency in currency_data:
        try:
            currency_obj: Currency = get_currency_by_code(
                currency.get("currency_code")
            )  # Generates a currency object based on the code
            currency_name = (
                currency_obj.name
            )  # Grab the name from the object made above

            transformed_row = {
                "currency_id": currency.get("currency_id"),
                "currency_code": currency.get("currency_code"),
                "currency_name": currency_name,
            }
            dim_currency.append(transformed_row)
        except Exception as error:
            raise Exception(
                f"Error processing row{currency.get("location_id")}: {error}"
            )
    return dim_currency


def convert_into_parquet(data):
    try:
        df = pd.DataFrame(data)

        buffer = BytesIO()

        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer)

        buffer.seek(0)

        return buffer
    except Exception as error:
        raise Exception(f"Could not convert to parquet. Error: {error}")
