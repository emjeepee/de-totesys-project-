import os
from pg8000.native import Connection

# from pg8000.dbapi import Connection as RemoteConnection


# PG_totesys_USERNAME=project_team_011
# PG_totesys_PASSWORD=WcdY1lIbOq5r853
# PG_totesys_DATABASE=totesys
# PG_totesys_HOST=nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com
# PG_totesys_PORT=5432



def mytest_conn_to_db():
    username = 'project_team_011'
    password = 'WcdY1lIbOq5r853'
    database = 'totesys'
    host = 'nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com'
    port = '5432'
    return Connection(
        username,
        database=database,
        password=password,
        host=host,
        port=port,
        ssl_context=True,
    )


def close_db(conn: Connection):
    conn.close()



my_Test_conn = mytest_conn_to_db()


query_result = my_Test_conn.run(
        f"SELECT column_name FROM information_schema.columns WHERE table_name = 'transaction' ORDER BY ordinal_position"
                           )
column_names = [col[0] for col in query_result] 
# ['transaction_id', 'transaction_type', 'sales_order_id', 'purchase_order_id', 'created_at', 'last_updated']

# print(column_names)
# print(type(query_result[0]))


result = my_Test_conn.run(
        f"""
        SELECT * FROM transaction
        WHERE last_updated > '2025-06-04T08:28:12' LIMIT 20;
        """
                    )
# result is:
# [ [20496, 'SALE', 14504, None, datetime.datetime(2025, 6, 4, 8, 58, 10, 6000), datetime.datetime(2025, 6, 4, 8, 58, 10, 6000)],
# [20497, 'SALE', 14505, None, datetime.datetime(2025, 6, 4, 9, 26, 9, 972000), datetime.datetime(2025, 6, 4, 9, 26, 9, 972000)],
# [20498, 'SALE', 14506, None, datetime.datetime(2025, 6, 4, 9, 29, 10, 166000), datetime.datetime(2025, 6, 4, 9, 29, 10, 166000)], etc  ]



print(f'There are {len(result)} updated rows in the transaction table')
print(f'column_names is this list: {column_names}')

