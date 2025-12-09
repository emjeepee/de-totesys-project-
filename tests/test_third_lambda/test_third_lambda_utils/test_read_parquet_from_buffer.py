import pytest
import duckdb


from io import BytesIO
from unittest.mock import Mock, patch, ANY
from datetime import datetime

from src.third_lambda.third_lambda_utils.read_parquet_from_buffer import read_parquet_from_buffer



# Set up a mock table:
@pytest.fixture(scope="function")
def setup():
    # Make a mock dimension date 
    # table as a Python list
    # of dictionaries:

    dim_date = []
    row_1 =  {                                                          
                "date_id": 1,
                "year": 'mock_year_1',
                "month": 'mock_month_1',
                "day": 'mock_day_1'
                    }
    
    row_2 =  {                                                          
                "date_id": 2,
                "year": 'mock_year_2',
                "month": 'mock_month_2',
                "day": 'mock_day_2'
            }


    row_3 =  {                                                          
                "date_id": 3,
                "year": 'mock_year_3',
                "month": 'mock_month_3',
                "day": 'mock_day_3'
            }

    dim_date.append(row_1)
    dim_date.append(row_2)
    dim_date.append(row_3)


    table_name = 'dim_date'


    cols = [ "date_id",
                "year", 
                "month",
                "day" 
            ]
    
    cols_str = '"date_id", "year", "month", "day"'

    conn = duckdb.connect()
    

    # Create test data:
    conn.execute("""
        CREATE TABLE dim_date (
            date_id INTEGER,
            year VARCHAR,
            month VARCHAR,
            day VARCHAR,
                            )
                """)



    conn.execute("""
        INSERT INTO dim_date VALUES
        (1, 'mock_year_1', 'mock_month_1', 'mock_day_1'),
        (2, 'mock_year_2', 'mock_month_2', 'mock_day_2'),
        (3, 'mock_year_3', 'mock_month_3', 'mock_day_3'),
                """)

    buffer = BytesIO()

    # get DuckDB to write 
    # Parquet data
    # directly to a file:
    with open("/tmp/test.parquet", "wb") as f:
        conn.execute(
            "COPY dim_date TO '/tmp/test.parquet' (FORMAT 'parquet')"
                    )

    # Load the parquet data 
    # in the file into a 
    # BytesIO buffer:
    with open("/tmp/test.parquet", "rb") as f:
        buffer.write(f.read())

    buffer.seek(0)
    conn.close()

    yield dim_date, table_name, cols, cols_str, buffer






def test_returns_duckdb_result(setup):
        # Arrange:
    dim_date, table_name, cols, cols_str, buffer = setup

    conn = duckdb.connect()

    result = read_parquet_from_buffer(buffer, conn)

    # DuckDB returns DuckDBPyRelation
    assert hasattr(result, "fetchall")