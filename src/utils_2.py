import boto3
import json
from datetime import datetime
import boto3.exceptions


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
    try:
        python_data = json.loads(json_data)
        return python_data
    except Exception as error:
        raise f"Error converting json to python: {error}"


def dt_splitter(input_dt):
    dt = datetime.fromisoformat(input_dt)
    date = dt.date().isoformat()
    time = dt.time().isoformat()
    return {"date": date, "time": time}


def transform_to_star_schema_fact_table(table_name, table_data):
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
            raise Exception(
                f"Error processing row{row.get("sales_order_id")}: {error}"
            )
    return fact_sales_order


def transform_to_dim_staff(staff_data, dept_data):
    dim_staff = []
    
    department_lookup = { # Allows to look at department table
        dept["department_id"]: {
        "department_name": dept.get("department_name"),
        "location": dept.get("location")
        } for dept in dept_data
    }

    for staff in staff_data:
        try:
            dept = department_lookup.get(staff.get("department_id")) # JOINS department table to staff at department ID
            transformed_row = {
                "staff_id": staff.get("staff_id"),
                "first_name": staff.get("first_name"),
                "last_name": staff.get("last_name"),
                "email_address": staff.get("email_address"),
                "department_name": dept.get("department_name"),
                "location": dept.get("location")
                }
            dim_staff.append(transformed_row)
        except Exception as error:
            raise Exception(
                f"Error processing row{staff.get("staff_id")}: {error}"
            )

    return dim_staff

def transform_to_dim_location(location_data):
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
                "phone": location.get("phone")
            }
            dim_location.append(transformed_row)
        except Exception as error:
            raise Exception(
                f"Error processing row{location.get("location_id")}: {error}"
            )
    return dim_location

def transform_to_dim_design(design_table):
    pass

def transform_to_dim_counterparty(counterparty_table):
    pass

def transform_to_dim_date(date_table):
    pass

def transform_to_dim_currency(currency_table):
    pass

def convert_into_parquet():
    pass
