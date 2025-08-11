from third_lambda.third_lambda_utils.third_lambda_init import third_lambda_init
from third_lambda.third_lambda_utils.make_pandas_dataframe import make_pandas_dataframe
from third_lambda.third_lambda_utils.make_SQL_queries import make_SQL_queries
from third_lambda.third_lambda_utils.make_SQL_queries_to_warehouse import make_SQL_queries_to_warehouse
from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db


def lambda_handler(event, context):
    """
    This function will:
        1) receive an event object from the processed
            bucket when the second lambda function 
            stores a dimension or the fact table 
            there as a Parquet file.
        2) respond to the event by first getting the 
            Parquet file from the processed bucket 
            and converting it to a Pandas dataframe.
        3) then extract data for a row from the 
            Pandas dataframe and convert that data 
            into an SQL query string. The query string
            will write the row to the appropriate 
            table in the warehouse. this function 
            will then append each query string  to a 
            list. The query strings will be of the 
            same type for all dimension tables as 
            updated row data from a dimension table 
            must replace outdated row data in the 
            warehouse. The fact table will require a 
            list of SQL query strings that add updated 
            rows to a table in the warehouse, keeping 
            the outdated rows.
        4) connect to the data warehouse and loop
            through the list to make the queries
            to the data warehouse.
        5) Close the connection to the warehouse.            

    Args:
        event: the event that triggers this lambda
         function.
        context: metadata about the environment in 
         which this lambda runs.

    Returns:
        None                            
    """

    # Get lookup table that contains 
    # values this lambda handler requires:
    lookup = third_lambda_init(event)    
    proc_bucket = lookup['proc_bucket']
    s3_client = lookup['s3_client']
    object_key = lookup['object_key']
    table_name = lookup['table_name']

    # Get the Parquet file and convert
    # it to a pandas dataframe:
    df = make_pandas_dataframe(proc_bucket, s3_client, object_key)

    # make the SQL queries from the 
    # data in the dataFrame:
    queries_list = make_SQL_queries(df, table_name)

    # Connnect to data warehouse:
    conn = conn_to_db("WAREHOUSE")

    # Make SQL queries to the data 
    # warehouse:
    make_SQL_queries_to_warehouse(queries_list, conn)

    # Close connection to warehouse:
    close_db(conn)


    return 








# OLD CODE:
# ingestion_bckt = "11-ingestion-bucket"
#     processed_bckt = event["detail"]["name"]

#     # create a dictionary that contains the
#     # pandas files relating to tables in
#     # the warehouse dictionary:
#     dataFrames_dict = get_pandas_dataFrames(
#         processed_bckt, ingestion_bckt, s3_client, "***timestamp***"
#     )

#     # dataFrames_dict will look
#     # like this:
#     # {
#     # 'dim_date': dim_table.parquet,
#     # ... ,
#     # 'facts_sales_order': facts_sales_order.parquet
#     # }, where each key is the name of a table
#     # in the warehouse database.

#     # get the data out of each of the
#     # files in the dictionary above and
#     # put the data in them into the
#     # warehouse database:
#     make_SQL_queries_to_warehouse(dataFrames_dict)


