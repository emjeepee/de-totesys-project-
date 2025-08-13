from src.first_lambda.first_lambda_utils.conn_to_db import conn_to_db, close_db











def get_stuff_from_tote_sys(table_name):
    conn = conn_to_db('TOTE_SYS')
    
    # query= f"SELECT * FROM {table_name}  LIMIT 2;"
    query= f'SELECT * FROM design ORDER BY design_id DESC LIMIT 3;'

    result = conn.run(query)

    close_db(conn)

    return result 
    
    
two_rows = get_stuff_from_tote_sys('design')    
print(two_rows)



# Returns this:
# [
#     [654, datetime.datetime(2025, 8, 12, 12, 11, 10, 73000), 'Fresh', '/Network', 'fresh-20240124-ap0b.json', datetime.datetime(2025, 8, 12, 12, 11, 10, 73000)], 
#     [653, datetime.datetime(2025, 8, 11, 17, 45, 9, 899000), 'Plastic', '/usr', 'plastic-20241006-e6nr.json', datetime.datetime(2025, 8, 11, 17, 45, 9, 899000)], 
#     [652, datetime.datetime(2025, 8, 11, 15, 5, 10, 119000), 'Frozen', '/home/user/dir', 'frozen-20240309-ohfo.json', datetime.datetime(2025, 8, 11, 15, 5, 10, 119000)]
# ]