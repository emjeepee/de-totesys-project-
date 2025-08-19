from src.third_lambda.third_lambda_utils.third_lambda_init              import third_lambda_init
from src.third_lambda.third_lambda_utils.make_pandas_dataframe          import make_pandas_dataframe
from src.third_lambda.third_lambda_utils.make_SQL_queries               import make_SQL_queries
from src.third_lambda.third_lambda_utils.make_SQL_queries_to_warehouse  import make_SQL_queries_to_warehouse


def third_lambda_handler(event, context):
    """
    This function:
        1) receives an event object from the processed
            bucket when the second lambda function 
            stores a dimension table or the fact table 
            there as a Parquet file.
        2) responds to the event by getting the 
            Parquet file from the processed bucket 
            and converting it to a Pandas dataFrame.
        3) then extracts data for a row from the 
            Pandas dataFrame and converts that data 
            into an SQL query string, appending each
            query string to a list. 
            Each query string will write a row to 
            the appropriate table in the warehouse. 
            The query strings for dimension tables
            ensure updated row data replaces 
            outdated row data. Query strings for the
            fact table add updated rows to the fact 
            table in the warehouse, ensuring the
            outdated rows remain.
        4) connects to the data warehouse and loops
            through the list of query strings to 
            make the queries to the warehouse.
        5) closes the connection to the warehouse.            

    Args:
        event: object genrated by AWS when the second 
         lambda handler writes a Parquet file to the 
         processed bucket.
        context: metadata about the environment in 
         which this lambda runs.

    Returns:
        None                            
    """

    # Get lookup table that contains 
    # values this lambda handler requires:
    lookup = third_lambda_init(event)    
    proc_bucket = lookup['proc_bucket'] # name of processed bucket
    s3_client = lookup['s3_client']     # boto3 S3 client object
    object_key = lookup['object_key']   # key under which processed bucket saved Parquet file
    table_name = lookup['table_name']   # name of table
    conn = lookup['conn']               # pg8000.native Connection object that knows about warehouse
    close_db = lookup['close_db']       # function to close connection to warehouse

    # Get the Parquet file and convert
    # it to a pandas dataframe:
    df = make_pandas_dataframe(proc_bucket, s3_client, object_key)

    # make the SQL queries from the 
    # data in the dataFrame:
    queries_list = make_SQL_queries(df, table_name)

    # Make SQL queries to the data 
    # warehouse:
    make_SQL_queries_to_warehouse(queries_list, conn)

    # Close connection to warehouse:
    close_db(conn)








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


